using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

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
            Func<CancellationToken, Task<Uri>> blobPathFactory,
            SourceTableRowItem sourceTableRowItem,
            long blockId,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
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
                blockItem = await ExportBlockAsync(blobPathFactory, blockItem, ct);
            }
        }

        private async Task<SourceBlockRowItem> ExportBlockAsync(
            Func<CancellationToken, Task<Uri>> blobPathFactory,
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
                blobPathFactory,
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

            var exportDetails = await exportClient.AwaitExportAsync(
                blockItem.IterationId,
                blockItem.SourceTable.TableName,
                operationId,
                ct);
            var urlItems = exportDetails
                .Select(e => new SourceUrlRowItem
                {
                    State = SourceUrlState.Exported,
                    SourceTable = blockItem.SourceTable,
                    IterationId = iteration.IterationId,
                    BlockId = blockItem.BlockId,
                    Url = e.BlobUri.ToString(),
                    RowCount = e.RecordCount
                });

            foreach (var urlItem in urlItems)
            {
                await RowItemGateway.AppendAsync(urlItem, ct);
            }
            blockItem = blockItem.ChangeState(SourceBlockState.Exported);
            await RowItemGateway.AppendAsync(blockItem, ct);

            return blockItem;
        }

        private async Task CleanUrlsAsync(SourceBlockRowItem blockItem, CancellationToken ct)
        {
            var existingUrls = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId]
                .BlockMap[blockItem.BlockId]
                .Urls;

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