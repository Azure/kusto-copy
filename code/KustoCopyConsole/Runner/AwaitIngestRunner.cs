using Azure.Core;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : RunnerBase
    {
        private const int MAXIMUM_EXTENT_MOVING = 100;

        public AwaitIngestRunner(
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
            while (!AllActivitiesCompleted())
            {
                await UpdateIngestedAsync(ct);
                await FailureDetectionAsync(ct);
                await MoveAsync(ct);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task UpdateIngestedAsync(CancellationToken ct)
        {
            async Task<IEnumerable<RowItemBase>> GetIngestionItemsAsync(
                ActivityRowItem activity,
                IterationRowItem iteration,
                IEnumerable<ActivityFlatHierarchy> items,
                string tempTableName,
                CancellationToken ct)
            {
                var dbClient = DbClientFactory.GetDbCommandClient(
                    activity.DestinationTable.ClusterUri,
                    activity.DestinationTable.DatabaseName);
                var allExtentRowCounts = await dbClient.GetExtentRowCountsAsync(
                    new KustoPriority(iteration.GetIterationKey()),
                    tempTableName,
                    ct);
                var extentRowCountByTags = allExtentRowCounts
                    .GroupBy(e => e.Tags)
                    .ToImmutableDictionary(g => g.Key);
                var ingestionItems = new List<RowItemBase>();

                Trace.TraceInformation($"AwaitIngest:  {allExtentRowCounts.Count} " +
                    $"extents found with {extentRowCountByTags.Count} tags");
                foreach (var item in items)
                {
                    if (extentRowCountByTags.TryGetValue(
                        item.Block.BlockTag,
                        out var extentRowCounts))
                    {
                        var targetRowCount = item
                            .Urls
                            .Sum(h => h.RowCount);
                        var blockExtentRowCount = extentRowCounts
                            .Sum(e => e.RecordCount);

                        if (blockExtentRowCount > targetRowCount)
                        {
                            throw new CopyException(
                                $"Target row count is {targetRowCount} while " +
                                $"we observe {blockExtentRowCount}",
                                false);
                        }
                        if (blockExtentRowCount == targetRowCount)
                        {
                            var extentItems = extentRowCounts
                                .Select(e => new ExtentRowItem
                                {
                                    ActivityName = activity.ActivityName,
                                    IterationId = iteration.IterationId,
                                    BlockId = item.Block.BlockId,
                                    ExtentId = e.ExtentId,
                                    RowCount = e.RecordCount
                                });

                            ingestionItems.AddRange(extentItems);
                            ingestionItems.Add(item.Block.ChangeState(BlockState.Ingested));
                        }
                    }
                }

                return ingestionItems;
            }
            var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                        a => a.RowItem.State != ActivityState.Completed,
                        i => i.RowItem.State != IterationState.Completed);
            var queuedBlocks = allBlocks
                .Where(h => h.Block.State == BlockState.Queued);
            var detectIngestionTasks = queuedBlocks
                .Where(h => h.TempTable != null)
                .GroupBy(h => h.TempTable)
                .Select(g => GetIngestionItemsAsync(
                    g.First().Activity,
                    g.First().Iteration,
                    g,
                    g.Key!.TempTableName,
                    ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(detectIngestionTasks);

            var ingestionItems = detectIngestionTasks
                .SelectMany(t => t.Result);

            //  We do wait for the ingested status to persist before moving
            //  This is to avoid moving extents before the confirmation of
            //  ingestion is persisted:  this would result in the block
            //  staying in "queued" if the process would restart
            await RowItemGateway.AppendAndPersistAsync(ingestionItems, ct);
        }

        private async Task FailureDetectionAsync(CancellationToken ct)
        {
            var activeIterations = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed);
            var iterationTasks = activeIterations
                .Select(i => IterationFailureDetectionAsync(i, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(iterationTasks);
        }

        private async Task IterationFailureDetectionAsync(
            IterationCache iterationCache,
            CancellationToken ct)
        {
            var queuedBlocks = iterationCache
                .BlockMap
                .Values
                .Where(b => b.RowItem.State == BlockState.Queued);

            if (queuedBlocks.Any())
            {
                var activityItem = RowItemGateway.InMemoryCache
                    .ActivityMap[iterationCache.RowItem.ActivityName]
                    .RowItem;
                var ingestClient = DbClientFactory.GetIngestClient(
                    activityItem.DestinationTable.ClusterUri,
                    activityItem.DestinationTable.DatabaseName,
                    iterationCache.TempTable!.TempTableName);
                var oldestBlock = queuedBlocks
                    .ArgMin(b => b.RowItem.Updated);

                foreach (var urlItem in oldestBlock.UrlMap.Values.Select(u => u.RowItem))
                {
                    var failure = await ingestClient.FetchIngestionFailureAsync(
                        urlItem.SerializedQueuedResult);

                    if (failure != null)
                    {
                        TraceWarning(
                            $"Warning!  Ingestion failed with status '{failure.Status}'" +
                            $"and detail '{failure.Details}' for blob {urlItem.Url} in block " +
                            $"{oldestBlock.RowItem.BlockId}, iteration " +
                            $"{oldestBlock.RowItem.IterationId}, activity " +
                            $"{oldestBlock.RowItem.ActivityName} ; block will be re-exported");
                        ReturnToPlanned(oldestBlock);

                        return;
                    }
                }
            }
        }

        private void ReturnToPlanned(BlockCache oldestBlock)
        {
            var newBlock = oldestBlock.RowItem.ChangeState(BlockState.Planned);

            newBlock.ExportOperationId = string.Empty;
            newBlock.BlockTag = string.Empty;
            RowItemGateway.Append(newBlock);
        }

        private async Task MoveAsync(CancellationToken ct)
        {
            var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var ingestedBlocks = allBlocks
                .Where(h => h.Block.State == BlockState.Ingested);
            var moveTasks = ingestedBlocks
                .GroupBy(h => h.Iteration.GetIterationKey())
                .Select(g => MoveBlocksFromIterationAsync(
                    g.First().Activity,
                    g.First().Iteration,
                    g.First().TempTable!.TempTableName,
                    g,
                    ct))
                .ToImmutableArray();

            Trace.TraceInformation($"AwaitIngest:  {moveTasks.Count()} extent moving commands");
            await TaskHelper.WhenAllWithErrors(moveTasks);
        }

        private async Task MoveBlocksFromIterationAsync(
            ActivityRowItem activity,
            IterationRowItem iteration,
            string tempTableName,
            IEnumerable<ActivityFlatHierarchy> items,
            CancellationToken ct)
        {
            var commandClient = DbClientFactory.GetDbCommandClient(
                activity.DestinationTable.ClusterUri,
                activity.DestinationTable.DatabaseName);
            var priority = new KustoPriority(iteration.GetIterationKey());
            var sortedItems = items
                .OrderBy(i => i.Block.BlockId)
                .ToImmutableArray();

            while (sortedItems.Any())
            {
                var movingItems = TakeMovingBlocks(sortedItems);
                var movingExtentIds = movingItems
                    .SelectMany(i => i.Extents.Select(e => e.ExtentId));
                var tags = movingItems
                    .Select(i => i.Block.BlockTag);
                var extentCount = await commandClient.MoveExtentsAsync(
                    priority,
                    tempTableName,
                    activity.DestinationTable.TableName,
                    movingExtentIds,
                    ct);
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    priority,
                    activity.DestinationTable.TableName,
                    tags,
                    ct);
                var newBlockItems = movingItems
                    .Select(i => i.Block.ChangeState(BlockState.ExtentMoved))
                    .ToImmutableArray();

                foreach(var item in newBlockItems)
                {
                    item.BlockTag = string.Empty;
                }
                RowItemGateway.Append(newBlockItems);
                sortedItems = sortedItems
                    .Skip(movingItems.Count())
                    .ToImmutableArray();
            }
        }

        private IEnumerable<ActivityFlatHierarchy> TakeMovingBlocks(
            IEnumerable<ActivityFlatHierarchy> items)
        {
            var i = 0;
            var totalExtents = 0;

            foreach (var item in items)
            {
                if (totalExtents + item.Extents.Count() > MAXIMUM_EXTENT_MOVING)
                {
                    return items.Take(Math.Max(1, i));
                }
                else
                {
                    totalExtents += item.Extents.Count();
                    ++i;
                }
            }

            return items;
        }
    }
}