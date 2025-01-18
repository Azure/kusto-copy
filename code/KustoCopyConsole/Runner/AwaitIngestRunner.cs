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
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(10))
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

                //  Sleep
                await SleepAsync(ct);
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