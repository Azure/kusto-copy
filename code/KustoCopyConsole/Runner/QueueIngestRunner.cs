using KustoCopyConsole.Entity;
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
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(
            IStagingBlobUriProvider blobPathProvider,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            if (blockItem.State == BlockState.Exported)
            {
                blockItem = await QueueIngestBlockAsync(blobPathProvider, blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> QueueIngestBlockAsync(
            IStagingBlobUriProvider blobPathProvider,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var iterationItem = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId];
            var urlItems = iterationItem
                .BlockMap[blockItem.BlockId]
                .UrlMap
                .Values
                .Select(u => u.RowItem);
            var tempTableName = iterationItem.RowItem.TempTableName;
            var ingestClient = DbClientFactory.GetIngestClient(
                blockItem.DestinationTable.ClusterUri,
                blockItem.DestinationTable.DatabaseName,
                tempTableName);
            var blockTag = $"kusto-copy:{Guid.NewGuid()}";
            var queueTasks = urlItems
                .Select(u => new Uri(u.Url))
                .Select(blobPathProvider.AuthorizeUri)
                .Select(url => ingestClient.QueueBlobAsync(url, blockTag, ct))
                .ToImmutableArray();

            await Task.WhenAll(queueTasks);

            blockItem = blockItem.ChangeState(BlockState.Queued);
            blockItem.BlockTag = blockTag;
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }
    }
}