using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
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

        public async Task RunAsync(
            IBlobPathProvider blobPathProvider,
            Task tempTableTask,
            TableRowItem sourceTableRowItem,
            long blockId,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            var queueIngestRunner = new QueueIngestRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);
            var blockItem = await EnsureBlockCreatedAsync(
                sourceTableRowItem,
                blockId,
                ingestionTimeStart,
                ingestionTimeEnd,
                ct);
            var exportClient = DbClientFactory.GetExportClient(
                blockItem.SourceTable.ClusterUri,
                blockItem.SourceTable.DatabaseName,
                blockItem.SourceTable.TableName);

            await tempTableTask;
            if (blockItem.State == BlockState.Exporting)
            {   //  The block is already exporting, so we track its progress
                exportClient.RegisterExistingOperation(blockItem.OperationId);
            }
            if (blockItem.State != BlockState.Exported)
            {
                await CleanUrlsAsync(blockItem, ct);
            }
            if (blockItem.State == BlockState.Planned)
            {
                blockItem = await ExportBlockAsync(blobPathProvider, exportClient, blockItem, ct);
            }
            if (blockItem.State == BlockState.Exporting)
            {
                blockItem = await AwaitExportBlockAsync(exportClient, blockItem, ct);
            }
            if (blockItem.State == BlockState.Exported)
            {   //  Ingest into destination
                await queueIngestRunner.RunAsync(
                    blobPathProvider,
                    blockItem,
                    ct);
            }
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
            foreach (var urlItem in urlItems)
            {
                await RowItemGateway.AppendAsync(urlItem, ct);
            }
            blockItem = blockItem.ChangeState(BlockState.Exported);
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task<BlockRowItem> ExportBlockAsync(
            IBlobPathProvider blobPathProvider,
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

        private async Task CleanUrlsAsync(BlockRowItem blockItem, CancellationToken ct)
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
        }

        private async Task<BlockRowItem> EnsureBlockCreatedAsync(
            TableRowItem tableRowItem,
            long blockId,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            var blockMap = RowItemGateway.InMemoryCache
                .SourceTableMap[tableRowItem.SourceTable]
                .IterationMap[tableRowItem.IterationId]
                .BlockMap;

            if (!blockMap.ContainsKey(blockId))
            {
                var newBlockItem = new BlockRowItem
                {
                    State = BlockState.Planned,
                    SourceTable = tableRowItem.SourceTable,
                    DestinationTable = tableRowItem.DestinationTable,
                    IterationId = tableRowItem.IterationId,
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