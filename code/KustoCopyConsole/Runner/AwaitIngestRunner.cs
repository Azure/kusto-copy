using Azure.Core;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : RunnerBase
    {
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

                foreach (var item in items)
                {
                    var extentRowCounts = extentRowCountByTags
                        .Where(e => e.Key.Contains(item.Block.BlockTag))
                        .Select(e => e.Value)
                        .FirstOrDefault();

                    if (extentRowCounts != null)
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

            await Task.WhenAll(detectIngestionTasks);

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

            await Task.WhenAll(iterationTasks);
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

            newBlock.OperationId = string.Empty;
            newBlock.BlockTag = string.Empty;
            RowItemGateway.Append(newBlock);
        }

        private async Task MoveAsync(CancellationToken ct)
        {
            async Task UpdateIngestedBlockAsync(ActivityFlatHierarchy item, CancellationToken ct)
            {
                var commandClient = DbClientFactory.GetDbCommandClient(
                    item.Activity.DestinationTable.ClusterUri,
                    item.Activity.DestinationTable.DatabaseName);
                var priority = new KustoPriority(item.Block.GetBlockKey());
                var extentCount = await commandClient.MoveExtentsAsync(
                    priority,
                    item.TempTable!.TempTableName,
                    item.Activity.DestinationTable.TableName,
                    item.Block.BlockTag,
                    ct);
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    priority,
                    item.Activity.DestinationTable.TableName,
                    item.Block.BlockTag,
                    ct);
                var newBlockItem = item.Block.ChangeState(BlockState.ExtentMoved);

                RowItemGateway.Append(newBlockItem);
            }

            var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var ingestedBlocks = allBlocks
                .Where(h => h.Block.State == BlockState.Ingested);
            var moveTasks = ingestedBlocks
                .OrderBy(h => h.Activity.ActivityName)
                .ThenBy(h => h.Block.IterationId)
                .ThenBy(h => h.Block.BlockId)
                .Select(h => UpdateIngestedBlockAsync(h, ct))
                .ToImmutableArray();

            await Task.WhenAll(moveTasks);
        }
    }
}