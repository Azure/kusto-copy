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
                    .Where(h => h.BlockItem.State == BlockState.Queued);
                var ingestionTasks = queuedBlocks
                    .Select(h => UpdateQueuedBlockAsync(h, ct))
                    .ToImmutableArray();

                await Task.WhenAll(ingestionTasks);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task UpdateQueuedBlockAsync(
            ActivityFlatHierarchy item,
            CancellationToken ct)
        {
            var iterationCache = RowItemGateway.InMemoryCache
                .ActivityMap[item.Activity.ActivityName]
                .IterationMap[item.Iteration.IterationId];
            var tempTableName = iterationCache.TempTable!.TempTableName;
            var targetRowCount = iterationCache.BlockMap[item.BlockItem.BlockId].UrlMap.Values
                .Sum(u => u.RowItem.RowCount);
            var dbClient = DbClientFactory.GetDbCommandClient(
                item.Activity.DestinationTable.ClusterUri,
                item.Activity.DestinationTable.DatabaseName);
            var rowCount = await dbClient.GetExtentRowCountAsync(
                new KustoPriority(item.BlockItem.GetIterationKey()),
                tempTableName,
                item.BlockItem.BlockTag,
                ct);

            if (rowCount > targetRowCount)
            {
                throw new CopyException(
                    $"Target row count is {targetRowCount} while we observe {rowCount}",
                    false);
            }
            if (rowCount == targetRowCount)
            {
                var newBlockItem = item.BlockItem.ChangeState(BlockState.Ingested);

                RowItemGateway.Append(newBlockItem);
            }
        }
    }
}