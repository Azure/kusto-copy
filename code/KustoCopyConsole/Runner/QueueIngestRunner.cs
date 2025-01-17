using Kusto.Ingest;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class QueueIngestRunner : RunnerBase
    {

        public QueueIngestRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                    a => a.RowItem.State != ActivityState.Completed,
                    i => i.RowItem.State != IterationState.Completed);
                var exportedBlocks = allBlocks
                    .Where(h => h.BlockItem.State == BlockState.Exported);
                var ingestionTasks = exportedBlocks
                    .Select(h => QueueIngestBlockAsync(h, ct));

                await Task.WhenAll(ingestionTasks);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task QueueIngestBlockAsync(ActivityFlatHierarchy item, CancellationToken ct)
        {
            var iterationCache = RowItemGateway.InMemoryCache
                .ActivityMap[item.Activity.ActivityName]
                .IterationMap[item.Iteration.IterationId];

            //  It's possible, although unlikely, the temp table hasn't been created yet
            //  If so, we'll process this block later
            if (iterationCache.TempTable != null)
            {
                var urlItems = iterationCache
                    .BlockMap[item.BlockItem.BlockId]
                    .UrlMap
                    .Values
                    .Select(u => u.RowItem);
                var ingestClient = DbClientFactory.GetIngestClient(
                    item.Activity.DestinationTable.ClusterUri,
                    item.Activity.DestinationTable.DatabaseName,
                    iterationCache.TempTable.TempTableName);
                var blockTag = $"kusto-copy:{Guid.NewGuid()}";
                var uriTasks = urlItems
                    .Select(u => StagingBlobUriProvider.AuthorizeUriAsync(new Uri(u.Url), ct))
                    .ToImmutableArray();

                await Task.WhenAll(uriTasks);

                var queueTasks = uriTasks
                    .Select(t => ingestClient.QueueBlobAsync(t.Result, blockTag, ct))
                    .ToImmutableArray();

                await Task.WhenAll(queueTasks);

                var newBlockItem = item.BlockItem.ChangeState(BlockState.Queued);

                newBlockItem.BlockTag = blockTag;
                RowItemGateway.Append(newBlockItem);
            }
        }
    }
}