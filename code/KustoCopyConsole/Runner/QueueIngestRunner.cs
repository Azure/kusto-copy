using Azure.Core;
using KustoCopyConsole.Entity.InMemory;
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
    internal class QueueIngestRunner : RunnerBase
    {

        public QueueIngestRunner(
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
                 TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            //  Clean half-queued URLs
            CleanQueuingUrls();
            while (!AllActivitiesCompleted())
            {
                var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                    a => a.RowItem.State != ActivityState.Completed,
                    i => i.RowItem.State != IterationState.Completed);
                var exportedBlocks = allBlocks
                    .Where(h => h.Block.State == BlockState.Exported);
                var ingestionTasks = exportedBlocks
                    .OrderBy(h => h.Activity.ActivityName)
                    .ThenBy(h => h.Block.IterationId)
                    .ThenBy(h => h.Block.BlockId)
                    .Select(h => QueueIngestBlockAsync(h, ct))
                    .ToImmutableArray();

                await TaskHelper.WhenAllWithErrors(ingestionTasks);

                if (!ingestionTasks.Any())
                {
                    //  Sleep
                    await SleepAsync(ct);
                }
            }
        }

        private void CleanQueuingUrls()
        {
            var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var queuingBlocks = allBlocks
                .Where(h => h.Block.State == BlockState.Exported)
                .Where(h => h.Urls.Any(u => u.State == UrlState.Queued));
            var queuingUrls = queuingBlocks
                .SelectMany(h => h.Urls);

            foreach (var block in queuingBlocks)
            {
                foreach (var url in block.Urls.Where(u => u.State == UrlState.Queued))
                {
                    var newUrlItem = url.ChangeState(UrlState.Exported);

                    RowItemGateway.Append(newUrlItem);
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
            UrlRowItem MarkUrlAsQueued(UrlRowItem url, string serializedQueueResult)
            {
                var newUrl = url.ChangeState(UrlState.Queued);

                newUrl.SerializedQueuedResult = serializedQueueResult;

                return newUrl;
            }

            //  It's possible, although unlikely, the temp table hasn't been created yet
            //  If so, we'll process this block later
            if (item.TempTable != null)
            {
                var ingestClient = DbClientFactory.GetIngestClient(
                    item.Activity.DestinationTable.ClusterUri,
                    item.Activity.DestinationTable.DatabaseName,
                    item.TempTable!.TempTableName);
                var blockTag = $"drop-by:kusto-copy|{Guid.NewGuid()}";
                var newBlockItem = item.Block.ChangeState(BlockState.Queued);

                newBlockItem.BlockTag = blockTag;
                Trace.TraceInformation($"Block {item.Block.GetBlockKey()}:  ingest " +
                    $"{item.Urls.Count()} urls");

                var queuingTasks = item
                    .Urls
                    .Select(u => new
                    {
                        Url = u,
                        Task = QueueIngestUrlAsync(
                            ingestClient,
                            newBlockItem,
                            new Uri(u.Url),
                            ct)
                    })
                    .ToImmutableArray();

                await TaskHelper.WhenAllWithErrors(queuingTasks.Select(o => o.Task));

                var newUrlItems = queuingTasks
                    .Select(o => MarkUrlAsQueued(o.Url, o.Task.Result));

                RowItemGateway.Append(newUrlItems);
                RowItemGateway.Append(newBlockItem);
                Trace.TraceInformation($"Block {item.Block.GetBlockKey()}:  " +
                    $"{item.Urls.Count()} urls queued");
            }
        }

        private async Task<string> QueueIngestUrlAsync(
            IngestClient ingestClient,
            BlockRowItem block,
            Uri blobUrl,
            CancellationToken ct)
        {
            var uri = await StagingBlobUriProvider.AuthorizeUriAsync(blobUrl, ct);
            var serializedQueueResult = await ingestClient.QueueBlobAsync(
                new KustoPriority(block.GetBlockKey()),
                uri,
                block.BlockTag,
                block.ExtentCreationTime,
                ct);

            return serializedQueueResult;
        }
    }
}