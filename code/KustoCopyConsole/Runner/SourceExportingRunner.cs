using Azure;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class SourceExportingRunner : RunnerBase
    {
        public SourceExportingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(
            IBlobPathProvider blobPathProvider,
            IImmutableDictionary<TableIdentity, Task> tempTableMap,
            SourceTableRowItem sourceTableRowItem,
            long blockId,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            var queueIngestRunner = new DestinationQueueIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);
            var blockItem = await EnsureBlockCreatedAsync(
                sourceTableRowItem,
                blockId,
                ingestionTimeStart,
                ingestionTimeEnd,
                ct);

            if (blockItem.State != SourceBlockState.Exported)
            {
                await CleanUrlsAsync(blockItem, ct);
            }
            if (blockItem.State == SourceBlockState.Planned)
            {
                blockItem = await ExportBlockAsync(blobPathProvider, blockItem, ct);
            }
            if (blockItem.State == SourceBlockState.Exporting)
            {
                blockItem = await AwaitExportBlockAsync(blockItem, ct);
            }
            if (blockItem.State == SourceBlockState.Exported)
            {   //  Ingest in all destinations
                var ingestionTasks = tempTableMap
                    .Select(pair => new
                    {
                        DestinationTable = pair.Key,
                        Task = pair.Value
                    })
                    .Select(o => queueIngestRunner.RunAsync(
                        blobPathProvider,
                        blockItem,
                        o.DestinationTable,
                        o.Task,
                        ct))
                    .ToImmutableArray();

                await Task.WhenAll(ingestionTasks);
            }
        }

        private async Task<SourceBlockRowItem> AwaitExportBlockAsync(
            SourceBlockRowItem blockItem,
            CancellationToken ct)
        {
            var exportClient = DbClientFactory.GetExportClient(
                blockItem.SourceTable.ClusterUri,
                blockItem.SourceTable.DatabaseName,
                blockItem.SourceTable.TableName);
            var exportDetails = await exportClient.AwaitExportAsync(
                blockItem.IterationId,
                blockItem.SourceTable.TableName,
                blockItem.OperationId,
                ct);
            var urlItems = exportDetails
                .Select(e => new SourceUrlRowItem
                {
                    State = SourceUrlState.Exported,
                    SourceTable = blockItem.SourceTable,
                    IterationId = blockItem.IterationId,
                    BlockId = blockItem.BlockId,
                    Url = e.BlobUri.ToString(),
                    RowCount = e.RecordCount
                });

            if (!urlItems.Any())
            {
                throw new InvalidDataException("No URL exported");
            }
            foreach (var urlItem in urlItems)
            {
                await RowItemGateway.AppendAsync(urlItem, ct);
            }
            blockItem = blockItem.ChangeState(SourceBlockState.Exported);
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<SourceBlockRowItem> ExportBlockAsync(
            IBlobPathProvider blobPathProvider,
            SourceBlockRowItem blockItem,
            CancellationToken ct)
        {
            var iteration = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .RowItem;
            var exportClient = DbClientFactory.GetExportClient(
                blockItem.SourceTable.ClusterUri,
                blockItem.SourceTable.DatabaseName,
                blockItem.SourceTable.TableName);
            var operationId = await exportClient.NewExportAsync(
                blobPathProvider,
                blockItem.IterationId,
                blockItem.BlockId,
                iteration.CursorStart,
                iteration.CursorEnd,
                blockItem.IngestionTimeStart,
                blockItem.IngestionTimeEnd,
                ct);

            blockItem = blockItem.ChangeState(SourceBlockState.Exporting);
            blockItem.OperationId = operationId;
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task CleanUrlsAsync(SourceBlockRowItem blockItem, CancellationToken ct)
        {
            var existingUrls = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .BlockMap[blockItem.BlockId]
                .UrlMap
                .Values;

            foreach (var url in existingUrls)
            {
                await RowItemGateway.AppendAsync(
                    url.RowItem.ChangeState(SourceUrlState.Deleted),
                    ct);
            }
        }

        private async Task<SourceBlockRowItem> EnsureBlockCreatedAsync(
            SourceTableRowItem sourceTableRowItem,
            long blockId,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            var blockMap = RowItemGateway.InMemoryCache
                .SourceTableMap[sourceTableRowItem.SourceTable]
                .IterationMap[sourceTableRowItem.IterationId]
                .BlockMap;

            if (!blockMap.ContainsKey(blockId))
            {
                var newBlockItem = new SourceBlockRowItem
                {
                    State = SourceBlockState.Planned,
                    SourceTable = sourceTableRowItem.SourceTable,
                    IterationId = sourceTableRowItem.IterationId,
                    BlockId = blockId,
                    IngestionTimeStart = ingestionTimeStart,
                    IngestionTimeEnd = ingestionTimeEnd
                };

                await RowItemGateway.AppendAsync(newBlockItem, ct);

                return newBlockItem;
            }
            else
            {
                return blockMap[blockId].RowItem;
            }
        }
    }
}