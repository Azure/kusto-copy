using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class PlanningRunner : RunnerBase
    {
        private const long RECORDS_PER_BLOCK = 8 * 1048576;

        public PlanningRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
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
                .Select(a => Task.Run(() => PlanActivityAsync(a, ct)))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);
        }

        private async Task PlanActivityAsync(string activityName, CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var newIteration = Database.Iterations.Query()
                    .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                    .Where(pf => pf.Equal(i => i.State, IterationState.Planning))
                    .FirstOrDefault();

                if (newIteration != null)
                {
                    await PlanIterationAsync(newIteration, ct);
                }

                //  Sleep
                //await SleepAsync(ct);
            }
        }

        private async Task PlanIterationAsync(
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            var activity = Parameterization.Activities[iterationRecord.IterationKey.ActivityName];
            var source = activity.Source.GetTableIdentity();
            var destination = activity.Destination.GetTableIdentity();
            var queryClient = DbClientFactory.GetDbQueryClient(
                source.ClusterUri,
                source.DatabaseName);
            var dbCommandClient = DbClientFactory.GetDbCommandClient(
                source.ClusterUri,
                source.DatabaseName);

            if (iterationRecord.State == IterationState.Starting)
            {
                var cursor = await queryClient.GetCurrentCursorAsync(
                    new KustoPriority(iterationRecord.IterationKey),
                    ct);

                using (var tx = Database.Database.CreateTransaction())
                {
                    iterationRecord = iterationRecord with
                    {
                        State = IterationState.Planning,
                        CursorEnd = cursor
                    };
                    UpdateIteration(iterationRecord);
                    Database.TempTables.AppendRecord(new TempTableRecord(
                        TempTableState.Required,
                        iterationRecord.IterationKey,
                        string.Empty));

                    tx.Complete();
                }
            }
            await ValidateIngestionTimeAsync(queryClient, activity, iterationRecord, ct);
            await PlanBlocksAsync(queryClient, dbCommandClient, activity, iterationRecord, ct);
        }

        private void UpdateIteration(
            IterationRecord iterationRecord,
            TransactionContext? txParam = null)
        {
            using (var tx = Database.Database.CreateTransaction())
            {
                txParam = txParam ?? tx;
                Database.Iterations.Query(txParam)
                    .Where(pf => pf.MatchKeys(
                        iterationRecord,
                        i => i.IterationKey.ActivityName,
                        i => i.IterationKey.IterationId))
                    .Delete();
                Database.Iterations.AppendRecord(iterationRecord, txParam);

                tx.Complete();
            }
        }

        private async Task ValidateIngestionTimeAsync(
            DbQueryClient queryClient,
            ActivityParameterization activity,
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            var hasNullIngestionTime = await queryClient.HasNullIngestionTime(
                new KustoPriority(iterationRecord.IterationKey),
                activity.Source.GetTableIdentity().TableName,
                activity.KqlQuery,
                ct);

            if (hasNullIngestionTime)
            {
                throw new CopyException(
                    $"Activity '{activity.ActivityName}' / Iteration" +
                    $" {iterationRecord.IterationKey.IterationId}:  null ingestion time are present." +
                    $"  Null ingestion time aren't supported.",
                    false);
            }
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            ActivityParameterization activity,
            IterationRecord iterationRecord,
            CancellationToken ct)
        {
            if (iterationRecord.State == IterationState.Planning)
            {
                var ingestionTimeInterval = await queryClient.GetIngestionTimeIntervalAsync(
                    new KustoPriority(iterationRecord.IterationKey),
                    activity.Source.GetTableIdentity().TableName,
                    activity.KqlQuery,
                    iterationRecord.CursorStart,
                    iterationRecord.CursorEnd,
                    ct);

                if (string.IsNullOrWhiteSpace(ingestionTimeInterval.MinIngestionTime)
                    || string.IsNullOrWhiteSpace(ingestionTimeInterval.MaxIngestionTime))
                {   //  No ingestion time:  either no rows or no rows with ingestion time
                    iterationRecord = iterationRecord with
                    {
                        State = IterationState.Completed
                    };
                    UpdateIteration(iterationRecord);
                }
                else
                {
                    await PlanBlocksAsync(
                        queryClient,
                        dbCommandClient,
                        activity,
                        iterationRecord,
                        ingestionTimeInterval,
                        ct);
                }
            }
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            ActivityParameterization activity,
            IterationRecord iterationRecord,
            IngestionTimeInterval ingestionTimeInterval,
            CancellationToken ct)
        {
            //  Do blocks one batch at the time until completion
            while (iterationRecord.State == IterationState.Planning)
            {
                var lastBlock = Database.Blocks.Query()
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.ActivityName,
                        iterationRecord.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationId,
                        iterationRecord.IterationKey.IterationId))
                    .OrderByDesc(b => b.BlockKey.BlockId)
                    .Take(1)
                    .FirstOrDefault();
                var hasReachedUpperIngestionTime = await PlanBlocksBatchAsync(
                    activity,
                    iterationRecord,
                    lastBlock == null ? 1 : lastBlock.BlockKey.BlockId + 1,
                    lastBlock?.IngestionTimeEnd.ToString(),
                    lastBlock?.IngestionTimeEnd.ToString() ?? ingestionTimeInterval.MinIngestionTime,
                    ingestionTimeInterval.MaxIngestionTime,
                    queryClient,
                    dbCommandClient,
                    ct);

                if (hasReachedUpperIngestionTime)
                {
                    iterationRecord = iterationRecord with
                    {
                        State = IterationState.Planned
                    };
                    UpdateIteration(iterationRecord);

                    return;
                }
            }
        }

        private async Task<bool> PlanBlocksBatchAsync(
            ActivityParameterization activity,
            IterationRecord iterationRecord,
            long nextBlockId,
            string? lastIngestionTime,
            string lowerIngestionTime,
            string upperIngestionTime,
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            CancellationToken ct)
        {
            var distribution = await queryClient.GetRecordDistributionAsync(
                new KustoPriority(iterationRecord.IterationKey),
                activity.Source.GetTableIdentity().TableName,
                activity.KqlQuery,
                iterationRecord.CursorStart,
                iterationRecord.CursorEnd,
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
                    activity,
                    iterationRecord,
                    nextBlockId,
                    lastIngestionTime,
                    lowerIngestionTime,
                    upperIngestionTime,
                    queryClient,
                    dbCommandClient,
                    ct);
            }
            else if (distribution.RecordGroups.Any())
            {
                var blockRecords = distribution.RecordGroups
                    .Select(r => new BlockRecord(
                        BlockState.Planned,
                        new BlockKey(
                            iterationRecord.IterationKey.ActivityName,
                            iterationRecord.IterationKey.IterationId,
                            nextBlockId++),
                        r.IngestionTimeStart,
                        r.IngestionTimeEnd,
                        r.MinCreatedOn!.Value,
                        r.MaxCreatedOn!.Value,
                        r.RowCount,
                        0,
                        string.Empty,
                        string.Empty))
                    .ToImmutableArray();

                Database.Blocks.AppendRecords(blockRecords);
            }

            return distribution.HasReachedUpperIngestionTime;
        }
    }
}