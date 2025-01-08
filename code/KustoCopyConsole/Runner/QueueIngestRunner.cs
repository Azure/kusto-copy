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
            IBlobPathProvider blobPathProvider,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var awaitIngestRunner = new DestinationAwaitIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            if (blockItem.State == BlockState.Exported)
            {
                blockItem = await QueueIngestBlockAsync(blobPathProvider, blockItem, ct);
            }
            blockItem = await awaitIngestRunner.RunAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<BlockRowItem> QueueIngestBlockAsync(
            IBlobPathProvider blobPathProvider,
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
            var authorizeTasks = urlItems
                .Select(u => new Uri(u.Url))
                .Select(url => new
                {
                    Url = url,
                    Task = blobPathProvider.AuthorizeUriAsync(url, ct)
                })
                .ToImmutableArray();

            await Task.WhenAll(authorizeTasks.Select(o => o.Task));

            var queueTasks = authorizeTasks
                .Select(o => ingestClient.QueueBlobAsync(o.Task.Result, blockTag, ct))
                .ToImmutableArray();

            await Task.WhenAll(queueTasks);

            blockItem = blockItem.ChangeState(BlockState.Queued);
            blockItem.BlockTag = blockTag;
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }
    }
}