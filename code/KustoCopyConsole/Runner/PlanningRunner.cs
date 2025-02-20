using Azure.Core;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class PlanningRunner : RunnerBase
    {
        #region Inner Types
        private record ProtoBlock(
            DateTime IngestionTimeStart,
            DateTime IngestionTimeEnd,
            long RowCount,
            DateTime? MinCreationTime,
            DateTime? MaxCreationTime)
        {
            public ProtoBlock Merge(ProtoBlock other)
            {
                return new ProtoBlock(
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

        private class ProtoBlockCollection
        {
            private readonly IImmutableList<ProtoBlock> _completedBlocks;
            private readonly ProtoBlock? _remainingBlock;

            private ProtoBlockCollection(
                IEnumerable<ProtoBlock> completedBlocks,
                ProtoBlock? remainingBlock)
            {
                _completedBlocks = completedBlocks.ToImmutableArray();
                _remainingBlock = remainingBlock;
            }

            public static ProtoBlockCollection Empty { get; }
                = new(Array.Empty<ProtoBlock>(), null);

            public DateTime? LastIngestionTimeEnd()
            {
                var allBlocks = _remainingBlock == null
                    ? _completedBlocks
                    : _completedBlocks.Append(_remainingBlock);
                var lastIngestionTimeEndBlock = allBlocks.Any()
                    ? allBlocks.ArgMax(b => b.IngestionTimeEnd)
                    : null;

                return lastIngestionTimeEndBlock?.IngestionTimeEnd;
            }

            public ProtoBlockCollection Add(IEnumerable<ProtoBlock> blocks)
            {
                var remainingBlocks = _remainingBlock == null
                    ? blocks
                    : blocks.Prepend(_remainingBlock);

                //  We sort descending since the stack serves them upside-down
                var stack = new Stack<ProtoBlock>(remainingBlocks
                    .OrderByDescending(d => d.IngestionTimeStart)
                    .ThenByDescending(d => d.IngestionTimeEnd));
                var completedBlocks = new List<ProtoBlock>(_completedBlocks);

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
                            completedBlocks.Add(first);
                            stack.Push(second);
                        }
                    }
                    else
                    {   //  Only way to get out if you got into the while loop
                        return new ProtoBlockCollection(completedBlocks, first);
                    }
                }

                //  If you didn't get into the while loop, there was no remaining block
                return new ProtoBlockCollection(completedBlocks, null);
            }

            public (IEnumerable<ProtoBlock> Blocks, ProtoBlockCollection Collection)
                PopCompletedBlocks(bool includeIncompleteBlock)
            {
                if (includeIncompleteBlock)
                {
                    return (
                        _remainingBlock == null
                        ? _completedBlocks
                        : _completedBlocks.Append(_remainingBlock),
                        ProtoBlockCollection.Empty);
                }
                else
                {
                    return (
                        _completedBlocks,
                        new ProtoBlockCollection(Array.Empty<ProtoBlock>(), _remainingBlock));
                }
            }
        }

        private record BatchExportBlock(
            IEnumerable<Task> exportingTasks,
            long nextBlockId,
            DateTime? nextIngestionTimeStart);
        #endregion

        private const int MAX_STATS_COUNT = 250000;
        private const long RECORDS_PER_BLOCK = 1048576;

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
                 TimeSpan.FromSeconds(10))
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

        protected override bool IsWakeUpRelevant(RowItemBase item)
        {
            return item is IterationRowItem i
                && i.State == IterationState.Starting;
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
            var protoBlocks = ProtoBlockCollection.Empty;

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
                var newProtoBlocks = await GetProtoBlockAsync(
                    iterationItem,
                    protoBlocks.LastIngestionTimeEnd() ?? lastBlock?.IngestionTimeEnd,
                    queryClient,
                    dbCommandClient,
                    ct);

                protoBlocks = protoBlocks.Add(newProtoBlocks);
                Trace.TraceInformation($"Planning {iterationItem.GetIterationKey()}:  " +
                    $"{newProtoBlocks.Count} new protoblocks compacted into " +
                    $"{protoBlocks.PopCompletedBlocks(false).Blocks.Count()} blocks");
                protoBlocks = PlanBlockBatch(
                    protoBlocks,
                    !newProtoBlocks.Any(),
                    iterationItem.ActivityName,
                    iterationItem.IterationId,
                    lastBlock?.BlockId ?? 0);

                if (!newProtoBlocks.Any())
                {
                    var isAnyBlock = RowItemGateway.InMemoryCache
                        .ActivityMap[iterationItem.ActivityName]
                        .IterationMap[iterationItem.IterationId]
                        .BlockMap
                        .Any();

                    iterationItem = isAnyBlock
                        ? iterationItem.ChangeState(IterationState.Planned)
                        : iterationItem.ChangeState(IterationState.Completed);
                    RowItemGateway.Append(iterationItem);
                }
            }
        }

        private ProtoBlockCollection PlanBlockBatch(
            ProtoBlockCollection protoBlocks,
            bool includeIncompleteBlock,
            string activityName,
            long iterationId,
            long lastBlockId)
        {
            (var blocks, var newProtoBlocks) =
                protoBlocks.PopCompletedBlocks(includeIncompleteBlock);

            foreach (var block in blocks)
            {
                var blockItem = new BlockRowItem
                {
                    State = BlockState.Planned,
                    ActivityName = activityName,
                    IterationId = iterationId,
                    BlockId = ++lastBlockId,
                    IngestionTimeStart = block.IngestionTimeStart,
                    IngestionTimeEnd = block.IngestionTimeEnd,
                    ExtentCreationTime = block.MinCreationTime,
                    PlannedRowCount = block.RowCount
                };

                RowItemGateway.Append(blockItem);
            }

            return newProtoBlocks;
        }

        //  Merge results from query + show extents command
        private async Task<IImmutableList<ProtoBlock>> GetProtoBlockAsync(
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
                    var protoBlocks = distributions
                        .Select(d =>
                        {
                            extentDateMap.TryGetValue(d.ExtentId, out DateTime creationTime);

                            return new ProtoBlock(
                                d.IngestionTimeStart,
                                d.IngestionTimeEnd,
                                d.RowCount,
                                creationTime,
                                creationTime);
                        })
                        .ToImmutableArray();

                    return protoBlocks;
                }
                else
                {
                    return await GetProtoBlockAsync(
                        iterationItem,
                        ingestionTimeStart,
                        queryClient,
                        dbCommandClient,
                        ct);
                }
            }
            else
            {
                return ImmutableArray<ProtoBlock>.Empty;
            }
        }
    }
}