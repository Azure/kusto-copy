using Kusto.Ingest;
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
                    .Where(h => h.Block.State == BlockState.Exported);
                var s = new Stopwatch(); s.Start();
                var ingestionTasks = exportedBlocks
                    .Select(h => QueueIngestBlockAsync(h, ct))
                    .ToImmutableArray();

                await Task.WhenAll(ingestionTasks);

                if (!ingestionTasks.Any())
                {
                    //  Sleep
                    await SleepAsync(ct);
                }
            }
        }

        protected override bool IsWakeUpRelevant(RowItemBase item)
        {
            return item is BlockRowItem b
                && b.State == BlockState.Exported;
        }

        private async Task QueueIngestBlockAsync(ActivityFlatHierarchy item, CancellationToken ct)
        {
            //  It's possible, although unlikely, the temp table hasn't been created yet
            //  If so, we'll process this block later
            if (item.TempTable != null)
            {
                var ingestClient = DbClientFactory.GetIngestClient(
                    item.Activity.DestinationTable.ClusterUri,
                    item.Activity.DestinationTable.DatabaseName,
                    item.TempTable!.TempTableName);
                var blockTag = $"kusto-copy:{Guid.NewGuid()}";
                var uriTasks = item
                    .Urls
                    .Select(u => StagingBlobUriProvider.AuthorizeUriAsync(new Uri(u.Url), ct))
                    .ToImmutableArray();

                await Task.WhenAll(uriTasks);

                var queueTasks = uriTasks
                    .Select(t => ingestClient.QueueBlobAsync(
                        t.Result,
                        blockTag,
                        item.Block.ExtentCreationTime,
                        ct))
                    .ToImmutableArray();

                await Task.WhenAll(queueTasks);

                var newBlockItem = item.Block.ChangeState(BlockState.Queued);

                newBlockItem.BlockTag = blockTag;
                RowItemGateway.Append(newBlockItem);
            }
        }
    }
}