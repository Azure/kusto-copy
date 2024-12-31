using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using YamlDotNet.Core.Tokens;

namespace KustoCopyConsole.Runner
{
    internal class DestinationQueueIngestRunner : RunnerBase
    {
        public DestinationQueueIngestRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(
            IBlobPathProvider blobPathProvider,
            SourceBlockRowItem sourceBlockItem,
            TableIdentity destinationTable,
            Task tempTableCreationTask,
            CancellationToken ct)
        {
            await tempTableCreationTask;

            var blockItem = await EnsureBlockCreatedAsync(
                sourceBlockItem,
                destinationTable,
                ct);
            var awaitIngestRunner = new DestinationAwaitIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);

            if (blockItem.State == DestinationBlockState.Queuing)
            {
                blockItem = await QueueIngestBlockAsync(blobPathProvider, blockItem, ct);
            }
            blockItem = await awaitIngestRunner.RunAsync(blockItem, ct);
        }

        private async Task<DestinationBlockRowItem> QueueIngestBlockAsync(
            IBlobPathProvider blobPathProvider,
            DestinationBlockRowItem blockItem,
            CancellationToken ct)
        {
            var urlItems = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .BlockMap[blockItem.BlockId]
                .UrlMap
                .Values
                .Select(u => u.RowItem);
            var tempTableName = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .DestinationMap[blockItem.DestinationTable]
                .RowItem
                .TempTableName;
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

            blockItem = blockItem.ChangeState(DestinationBlockState.Queued);
            blockItem.BlockTag = blockTag;
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<DestinationBlockRowItem> EnsureBlockCreatedAsync(
            SourceBlockRowItem sourceBlockItem,
            TableIdentity destinationTable,
            CancellationToken ct)
        {
            var blockMap = RowItemGateway.InMemoryCache
                .SourceTableMap[sourceBlockItem.SourceTable]
                .IterationMap[sourceBlockItem.IterationId]
                .DestinationMap[destinationTable]
                .BlockMap;

            if (!blockMap.ContainsKey(sourceBlockItem.BlockId))
            {
                var newBlockItem = new DestinationBlockRowItem
                {
                    State = DestinationBlockState.Queuing,
                    SourceTable = sourceBlockItem.SourceTable,
                    DestinationTable = destinationTable,
                    IterationId = sourceBlockItem.IterationId,
                    BlockId = sourceBlockItem.BlockId
                };

                await RowItemGateway.AppendAsync(newBlockItem, ct);

                return newBlockItem;
            }
            else
            {
                return blockMap[sourceBlockItem.BlockId].RowItem;
            }
        }
    }
}