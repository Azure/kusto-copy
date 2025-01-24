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
        public AwaitIngestRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory,
           IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
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
                var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                    a => a.RowItem.State != ActivityState.Completed,
                    i => i.RowItem.State != IterationState.Completed);
                var queuedBlocks = allBlocks
                    .Where(h => h.Block.State == BlockState.Queued);
                var ingestionTasks = queuedBlocks
                    .Where(h => h.TempTable != null)
                    .GroupBy(h => h.TempTable)
                    .Select(g => UpdateQueuedBlocksAsync(g, ct))
                    .ToImmutableArray();

                await Task.WhenAll(ingestionTasks);
                await FailureDetectionAsync(ct);

                //  Sleep
                await SleepAsync(ct);
            }
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
            foreach (var url in oldestBlock.UrlMap.Values.Select(u => u.RowItem))
            {
                RowItemGateway.Append(url.ChangeState(UrlState.Deleted));
            }
        }

        private async Task UpdateQueuedBlocksAsync(
            IEnumerable<ActivityFlatHierarchy> items,
            CancellationToken ct)
        {
            var activity = items.First().Activity;
            var iteration = items.First().Iteration;
            var tempTableName = items.First().TempTable!.TempTableName;
            var dbClient = DbClientFactory.GetDbCommandClient(
                activity.DestinationTable.ClusterUri,
                activity.DestinationTable.DatabaseName);
            var extentRowCounts = await dbClient.GetExtentRowCountsAsync(
                new KustoPriority(iteration.GetIterationKey()),
                tempTableName,
                ct);

            foreach (var item in items)
            {
                var targetRowCount = item
                    .Urls
                    .Sum(h => h.RowCount);
                var blockExtentRowCount = extentRowCounts
                    .Where(e => e.Tags.Contains(item.Block.BlockTag))
                    .FirstOrDefault();

                if (blockExtentRowCount != null)
                {
                    if (blockExtentRowCount.RecordCount > targetRowCount)
                    {
                        throw new CopyException(
                            $"Target row count is {targetRowCount} while " +
                            $"we observe {blockExtentRowCount.RecordCount}",
                            false);
                    }
                    if (blockExtentRowCount.RecordCount == targetRowCount)
                    {
                        var newBlockItem = item.Block.ChangeState(BlockState.Ingested);

                        RowItemGateway.Append(newBlockItem);
                    }
                }
            }
        }
    }
}