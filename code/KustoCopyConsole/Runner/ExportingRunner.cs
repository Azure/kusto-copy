using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : RunnerBase
    {
        public ExportingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(
            IStagingBlobUriProvider blobPathProvider,
            IterationRowItem tableRowItem,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var exportClient = DbClientFactory.GetExportClient(
                blockItem.SourceTable.ClusterUri,
                blockItem.SourceTable.DatabaseName,
                blockItem.SourceTable.TableName);

            if (blockItem.State == BlockState.CompletingExport)
            {
                blockItem = await CleanUrlsAsync(blockItem, ct);
            }
            if (blockItem.State == BlockState.Planned)
            {
                blockItem = await ExportBlockAsync(blobPathProvider, exportClient, blockItem, ct);
            }
            if (blockItem.State == BlockState.Exporting)
            {
                blockItem = await AwaitExportBlockAsync(exportClient, blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> AwaitExportBlockAsync(
            ExportClient exportClient,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var exportDetails = await exportClient.AwaitExportAsync(
                blockItem.IterationId,
                blockItem.SourceTable.TableName,
                blockItem.OperationId,
                ct);
            var urlItems = exportDetails
                .Select(e => new UrlRowItem
                {
                    State = UrlState.Exported,
                    SourceTable = blockItem.SourceTable,
                    DestinationTable = blockItem.DestinationTable,
                    IterationId = blockItem.IterationId,
                    BlockId = blockItem.BlockId,
                    Url = e.BlobUri.ToString(),
                    RowCount = e.RecordCount
                });

            if (!urlItems.Any())
            {
                throw new InvalidDataException("No URL exported");
            }
            blockItem = blockItem.ChangeState(BlockState.CompletingExport);
            await RowItemGateway.AppendAsync(blockItem, ct);
            foreach (var urlItem in urlItems)
            {
                await RowItemGateway.AppendAsync(urlItem, ct);
            }
            blockItem = blockItem.ChangeState(BlockState.Exported);
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<BlockRowItem> ExportBlockAsync(
            IStagingBlobUriProvider blobPathProvider,
            ExportClient exportClient,
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var iteration = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .RowItem;
            var operationId = await exportClient.NewExportAsync(
                blobPathProvider,
                blockItem.IterationId,
                blockItem.BlockId,
                iteration.CursorStart,
                iteration.CursorEnd,
                blockItem.IngestionTimeStart,
                blockItem.IngestionTimeEnd,
                ct);

            blockItem = blockItem.ChangeState(BlockState.Exporting);
            blockItem.OperationId = operationId;
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<BlockRowItem> CleanUrlsAsync(
            BlockRowItem blockItem,
            CancellationToken ct)
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
                    url.RowItem.ChangeState(UrlState.Deleted),
                    ct);
            }
            blockItem = blockItem.ChangeState(BlockState.Exporting);
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }
    }
}