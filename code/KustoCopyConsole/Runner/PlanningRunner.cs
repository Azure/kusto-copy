using Azure.Core;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class PlanningRunner : RunnerBase
    {
        private const long RECORDS_PER_BLOCK = 8 * 1048576;

        public PlanningRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var tasks = Parameterization
                .Activities
                .Keys
                .Select(a => Task.Run(() => RunActivityAsync(a, ct)))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);
        }

        private async Task RunActivityAsync(string activityName, CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                if (RowItemGateway.InMemoryCache.ActivityMap.TryGetValue(
                    activityName,
                    out var activity))
                {
                    var newIterations = activity.IterationMap
                        .Values
                        .Select(i => i.RowItem)
                        .Where(i => i.State <= IterationState.Planning)
                        .Select(i => new
                        {
                            Key = i.GetIterationKey(),
                            Iteration = i
                        });

                    foreach (var o in newIterations)
                    {
                        await PlanIterationAsync(o.Iteration, ct);
                    }
                }
                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task PlanIterationAsync(
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache
                .ActivityMap[iterationItem.ActivityName]
                .RowItem;
            var queryClient = DbClientFactory.GetDbQueryClient(
                activity.SourceTable.ClusterUri,
                activity.SourceTable.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                activity.SourceTable.ClusterUri,
                activity.SourceTable.DatabaseName);

            if (iterationItem.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iterationItem.GetIterationKey()),
                    ct);

                iterationItem = iterationItem.ChangeState(IterationState.Planning);
                iterationItem.CursorEnd = cursor;
                RowItemGateway.Append(iterationItem);
            }
            await ValidateIngestionTimeAsync(queryClient, activity, iterationItem, ct);
            await PlanBlocksAsync(queryClient, dbCommandClient, iterationItem, ct);
        }

        private async Task ValidateIngestionTimeAsync(
            DbQueryClient queryClient,
            ActivityRowItem activity,
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            var activityParam = Parameterization.Activities[iterationItem.ActivityName];
            var hasNullIngestionTime = await queryClient.HasNullIngestionTime(
                new KustoPriority(iterationItem.GetIterationKey()),
                activity.SourceTable.TableName,
                activityParam.KqlQuery,
                ct);

            if (hasNullIngestionTime)
            {
                throw new CopyException(
                    $"Activity '{activity.ActivityName}' / Iteration" +
                    $" {iterationItem.IterationId}:  null ingestion time are present." +
                    $"  Null ingestion time aren't supported.",
                    false);
            }
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            IterationRowItem iterationItem,
            CancellationToken ct)
        {
            if (iterationItem.State == IterationState.Planning)
            {
                var activityItem = RowItemGateway.InMemoryCache
                    .ActivityMap[iterationItem.ActivityName]
                    .RowItem;
                var activityParam = Parameterization.Activities[iterationItem.ActivityName];
                var ingestionTimeInterval = await queryClient.GetIngestionTimeIntervalAsync(
                    new KustoPriority(iterationItem.GetIterationKey()),
                    activityItem.SourceTable.TableName,
                    activityParam.KqlQuery,
                    iterationItem.CursorStart,
                    iterationItem.CursorEnd,
                    ct);

                if (string.IsNullOrWhiteSpace(ingestionTimeInterval.MinIngestionTime)
                    || string.IsNullOrWhiteSpace(ingestionTimeInterval.MaxIngestionTime))
                {   //  No ingestion time:  either no rows or no rows with ingestion time
                    iterationItem = iterationItem.ChangeState(IterationState.Completed);
                    RowItemGateway.Append(iterationItem);
                }
                else
                {
                    //  Loop on block batches
                    while (iterationItem.State == IterationState.Planning)
                    {
                        var blockMap = RowItemGateway.InMemoryCache
                            .ActivityMap[iterationItem.ActivityName]
                            .IterationMap[iterationItem.IterationId]
                            .BlockMap;
                        var lastBlock = blockMap.Any()
                            ? blockMap.Values.ArgMax(b => b.RowItem.BlockId).RowItem
                            : null;
                        var hasReachedUpperIngestionTime = await PlanBlocksBatchAsync(
                            activityItem,
                            iterationItem,
                            activityParam,
                            lastBlock?.IngestionTimeEnd.ToString(),
                            lastBlock?.IngestionTimeEnd.ToString() ?? ingestionTimeInterval.MinIngestionTime,
                            ingestionTimeInterval.MaxIngestionTime,
                            queryClient,
                            dbCommandClient,
                            ct);

                        if (hasReachedUpperIngestionTime)
                        {
                            RowItemGateway.Append(iterationItem.ChangeState(IterationState.Planned));

                            return;
                        }
                    }
                }
            }
        }

        private async Task<bool> PlanBlocksBatchAsync(
            ActivityRowItem activityItem,
            IterationRowItem iterationItem,
            ActivityParameterization activityParam,
            string? lastIngestionTime,
            string lowerIngestionTime,
            string upperIngestionTime,
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            CancellationToken ct)
        {
            var distribution = await queryClient.GetRecordDistributionAsync(
                new KustoPriority(iterationItem.GetIterationKey()),
                activityItem.SourceTable.TableName,
                activityParam.KqlQuery,
                iterationItem.CursorStart,
                iterationItem.CursorEnd,
                lastIngestionTime,
                lowerIngestionTime,
                upperIngestionTime,
                RECORDS_PER_BLOCK,
                ct);

            //  Check for racing condition where extents got merged and extent ids didn't exist
            //  when retrieving extent creation date
            if (distribution.RecordGroups.Any(d => d.MinCreatedOn == null))
            {
                return await PlanBlocksBatchAsync(
                    activityItem,
                    iterationItem,
                    activityParam,
                    lastIngestionTime,
                    lowerIngestionTime,
                    upperIngestionTime,
                    queryClient,
                    dbCommandClient,
                    ct);
            }
            else if (distribution.RecordGroups.Any())
            {
                //var blockItem = new BlockRowItem
                //{
                //    State = BlockState.Planned,
                //    ActivityName = activityName,
                //    IterationId = iterationId,
                //    BlockId = ++lastBlockId,
                //    //IngestionTimeStart = block.IngestionTimeStart,
                //    //IngestionTimeEnd = block.IngestionTimeEnd,
                //    MinCreationTime = block.MinCreationTime,
                //    MaxCreationTime = block.MaxCreationTime,
                //    PlannedRowCount = block.RowCount
                //};
                throw new NotImplementedException();
            }

            return distribution.HasReachedUpperIngestionTime;
        }
    }
}