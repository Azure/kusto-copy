using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.RowItems.Keys;
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
        #region Inner Types
        private record RecordDistributionInExtent(
            DateTime IngestionTime,
            string ExtentId,
            long RowCount,
            DateTime? MinCreatedOn);

        private record BatchExportBlock(
            IEnumerable<Task> exportingTasks,
            long nextBlockId,
            DateTime? nextIngestionTimeStart);
        #endregion

        private static readonly TimeSpan WAKE_PERIOD = TimeSpan.FromMinutes(1);

        private const int MAX_STATS_COUNT = 1000;
        private const long RECORDS_PER_BLOCK = 1048576;

        public PlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var taskMap = new Dictionary<IterationKey, Task>();

            while (taskMap.Any() || !AllActivitiesCompleted())
            {
                var newIterations = RowItemGateway.InMemoryCache.ActivityMap
                     .Values
                     .SelectMany(a => a.IterationMap.Values)
                     .Select(i => i.RowItem)
                     .Where(i => i.State <= IterationState.Planning)
                     .Select(i => new
                     {
                         Key = i.GetIterationKey(),
                         Iteration = i
                     })
                     .Where(o => !taskMap.ContainsKey(o.Key));

                foreach (var o in newIterations)
                {
                    taskMap.Add(o.Key, PlanIterationAsync(o.Iteration, ct));
                }
                await CleanTaskMapAsync(taskMap);
                //  Sleep
                await SleepAsync(WAKE_PERIOD, ct);
            }
        }

        protected override bool IsWakeUpRelevant(RowItemBase item)
        {
            return item is IterationRowItem i
                && i.State == IterationState.Planning;
        }

        private async Task CleanTaskMapAsync(IDictionary<IterationKey, Task> taskMap)
        {
            foreach (var taskKey in taskMap.Keys.ToImmutableArray())
            {
                var task = taskMap[taskKey];

                if (task.IsCompleted)
                {
                    await task;
                    taskMap.Remove(taskKey);
                }
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
            await PlanBlocksAsync(queryClient, dbCommandClient, iterationItem, ct);
        }

        private async Task PlanBlocksAsync(
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            IterationRowItem iterationItem,
            CancellationToken ct)
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
                var distributionInExtents = await GetRecordDistributionInExtents(
                    iterationItem,
                    lastBlock?.IngestionTimeEnd,
                    queryClient,
                    dbCommandClient,
                    ct);

                if (distributionInExtents.Any())
                {
                    var orderedDistributionInExtents = distributionInExtents
                        .OrderBy(d => d.IngestionTime)
                        .ThenBy(d => d.MinCreatedOn)
                        .ToImmutableArray();

                    while (orderedDistributionInExtents.Any())
                    {
                        (var newBlockItem, var remainingDistributionInExtents) = PlanSingleBlock(
                            iterationItem,
                            lastBlock,
                            orderedDistributionInExtents);

                        orderedDistributionInExtents = remainingDistributionInExtents
                            .ToImmutableArray();
                        RowItemGateway.Append(newBlockItem);
                        lastBlock = newBlockItem;
                    }
                }
                else
                {
                    iterationItem = iterationItem.ChangeState(IterationState.Planned);
                    RowItemGateway.Append(iterationItem);
                }
            }
        }

        private (BlockRowItem, IEnumerable<RecordDistributionInExtent>) PlanSingleBlock(
            IterationRowItem iterationItem,
            BlockRowItem? lastBlock,
            IImmutableList<RecordDistributionInExtent> distributionInExtents)
        {
            var nextBlockId = (lastBlock?.BlockId ?? 0) + 1;
            var nextIngestionTimeStart = lastBlock?.IngestionTimeEnd;
            long cummulativeRowCount = 0;

            for (var i = 0; i != distributionInExtents.Count; ++i)
            {
                var distribution = distributionInExtents[i];

                cummulativeRowCount += distribution.RowCount;
                if (i + 1 == distributionInExtents.Count
                    || cummulativeRowCount + distributionInExtents[i + 1].RowCount > RECORDS_PER_BLOCK
                    || distribution.MinCreatedOn != distributionInExtents[i + 1].MinCreatedOn)
                {
                    var cummulativeDistributions = distributionInExtents.Take(i + 1);
                    var remainingDistributions = distributionInExtents.Skip(i + 1);
                    var blockItem = new BlockRowItem
                    {
                        State = BlockState.Planned,
                        ActivityName = iterationItem.ActivityName,
                        IterationId = iterationItem.IterationId,
                        BlockId = nextBlockId++,
                        IngestionTimeStart = cummulativeDistributions.Min(d => d.IngestionTime),
                        IngestionTimeEnd = cummulativeDistributions.Max(d => d.IngestionTime)
                    };

                    return (blockItem, remainingDistributions);
                }
            }

            throw new InvalidOperationException("We should never reach this code");
        }

        //  Merge results from query + show extents command
        private async Task<IImmutableList<RecordDistributionInExtent>> GetRecordDistributionInExtents(
            IterationRowItem iterationItem,
            DateTime? ingestionTimeStart,
            DbQueryClient queryClient,
            DbCommandClient dbCommandClient,
            CancellationToken ct)
        {
            var activityItem = RowItemGateway.InMemoryCache
                .ActivityMap[iterationItem.ActivityName]
                .RowItem;
            var activityParam = Parameterization.Activities[iterationItem.ActivityName];
            var distributions = await queryClient.GetRecordDistributionAsync(
                new KustoPriority(iterationItem.ActivityName, iterationItem.IterationId),
                activityItem.SourceTable.TableName,
                activityParam.KqlQuery,
                iterationItem.CursorStart,
                iterationItem.CursorEnd,
                ingestionTimeStart,
                MAX_STATS_COUNT,
                ct);

            if (distributions.Any())
            {
                var extentIds = distributions
                    .Select(d => d.ExtentId)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct();
                var extentDates = await dbCommandClient.GetExtentDatesAsync(
                    new KustoPriority(iterationItem.ActivityName, iterationItem.IterationId),
                    activityItem.SourceTable.TableName,
                    extentIds,
                    ct);

                //  Check for racing condition where extents got merged and extent ids don't exist
                //  between 2 queries
                if (extentDates.Count == extentIds.Count())
                {
                    var distributionInExtents = distributions
                        .GroupJoin(
                        extentDates,
                        d => d.ExtentId, e => e.ExtentId,
                        (left, rightGroup) => new RecordDistributionInExtent(
                            left.IngestionTime,
                            left.ExtentId,
                            left.RowCount,
                            rightGroup.FirstOrDefault()?.MinCreatedOn))
                        .ToImmutableArray();

                    return distributionInExtents;
                }
                else
                {
                    return await GetRecordDistributionInExtents(
                        iterationItem,
                        ingestionTimeStart,
                        queryClient,
                        dbCommandClient,
                        ct);
                }
            }
            else
            {
                return ImmutableArray<RecordDistributionInExtent>.Empty;
            }
        }
    }
}