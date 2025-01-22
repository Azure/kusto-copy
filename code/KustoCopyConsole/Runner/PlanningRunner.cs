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
            DateTime IngestionTimeStart,
            DateTime IngestionTimeEnd,
            long RowCount,
            DateTime? MinCreationTime,
            DateTime? MaxCreationTime)
        {
            public RecordDistributionInExtent Merge(RecordDistributionInExtent other)
            {
                return new RecordDistributionInExtent(
                    Min(IngestionTimeStart, other.IngestionTimeStart),
                    Max(IngestionTimeEnd, other.IngestionTimeEnd),
                    RowCount + other.RowCount,
                    MinWithNull(MinCreationTime, MaxCreationTime),
                    MaxWithNull(MinCreationTime, MaxCreationTime));
            }

            public TimeSpan? CreationTimeDelta => MaxCreationTime - MinCreationTime;

            private static DateTime? MinWithNull(DateTime? a, DateTime? b)
            {
                return a == null || b == null
                    ? null
                    : Min(a.Value, b.Value);
            }

            private static DateTime Min(DateTime a, DateTime b)
            {
                return a < b ? a : b;
            }

            private static DateTime? MaxWithNull(DateTime? a, DateTime? b)
            {
                return a == null || b == null
                    ? null
                    : Max(a.Value, b.Value);
            }

            private static DateTime Max(DateTime a, DateTime b)
            {
                return a > b ? a : b;
            }
        }

        private record BatchExportBlock(
            IEnumerable<Task> exportingTasks,
            long nextBlockId,
            DateTime? nextIngestionTimeStart);
        #endregion

        private const int MAX_STATS_COUNT = 1000;
        private const long RECORDS_PER_BLOCK = 1048576;

        public PlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(10))
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
                await SleepAsync(ct);
            }
        }

        protected override bool IsWakeUpRelevant(RowItemBase item)
        {
            return item is IterationRowItem i
                && i.State == IterationState.Starting;
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
                    PlanBlockBatch(
                        distributionInExtents,
                        iterationItem.ActivityName,
                        iterationItem.IterationId,
                        lastBlock?.BlockId ?? 0);
                }
                else
                {
                    iterationItem = blockMap.Any()
                        ? iterationItem.ChangeState(IterationState.Planned)
                        : iterationItem.ChangeState(IterationState.Completed);
                    RowItemGateway.Append(iterationItem);
                }
            }
        }

        private void PlanBlockBatch(
            IEnumerable<RecordDistributionInExtent> distributionInExtents,
            string activityName,
            long iterationId,
            long lastBlockId)
        {
            void CreateBlock(
                RecordDistributionInExtent distribution,
                string activityName,
                long iterationId,
                long blockId)
            {
                var block = new BlockRowItem
                {
                    State = BlockState.Planned,
                    ActivityName = activityName,
                    IterationId = iterationId,
                    BlockId = blockId,
                    IngestionTimeStart = distribution.IngestionTimeStart,
                    IngestionTimeEnd = distribution.IngestionTimeEnd,
                    ExtentCreationTime = distribution.MinCreationTime,
                    PlannedRowCount = distribution.RowCount
                };

                RowItemGateway.Append(block);
            }

            //  We sort descending since the stack serves them upside-down
            var stack = new Stack<RecordDistributionInExtent>(distributionInExtents
                .OrderByDescending(d => d.IngestionTimeStart)
                .ThenByDescending(d => d.IngestionTimeEnd));

            while (stack.Any())
            {
                var first = stack.Pop();

                if (stack.Any())
                {
                    var second = stack.Pop();
                    var merge = first.Merge(second);

                    if (first.IngestionTimeEnd == second.IngestionTimeStart
                        || (merge.RowCount <= RECORDS_PER_BLOCK
                        && (merge.CreationTimeDelta < TimeSpan.FromDays(1)
                        || second.MinCreationTime == null && first.MinCreationTime == null)))
                    {   //  We merge
                        stack.Push(merge);
                    }
                    else
                    {   //  We don't merge
                        CreateBlock(first, activityName, iterationId, ++lastBlockId);
                        stack.Push(second);
                    }
                }
                else
                {
                    CreateBlock(first, activityName, iterationId, ++lastBlockId);
                }
            }
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
                new KustoPriority(iterationItem.GetIterationKey()),
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
                    //  Exclude the empty extent-id (for row-store rows)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct();
                var extentDates = await dbCommandClient.GetExtentDatesAsync(
                    new KustoPriority(iterationItem.GetIterationKey()),
                    activityItem.SourceTable.TableName,
                    extentIds,
                    ct);
                var extentDateMap = extentDates
                    .ToImmutableDictionary(e => e.ExtentId, e => e.MinCreatedOn);

                //  Check for racing condition where extents got merged and extent ids don't exist
                //  between 2 queries
                if (extentDates.Count == extentIds.Count())
                {
                    var distributionInExtents = distributions
                        .Select(d =>
                        {
                            extentDateMap.TryGetValue(d.ExtentId, out DateTime creationTime);

                            return new RecordDistributionInExtent(
                                d.IngestionTimeStart,
                                d.IngestionTimeEnd,
                                d.RowCount,
                                creationTime,
                                creationTime);
                        })
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