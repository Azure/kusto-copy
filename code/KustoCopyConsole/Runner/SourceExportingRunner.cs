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
                blockItem = await ExportBlockAsync(blockItem, ct);
            }
        }

        private async Task<SourceBlockRowItem> ExportBlockAsync(
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
                iteration.CursorStart,
                iteration.CursorEnd,
                blockItem.IngestionTimeStart,
                blockItem.IngestionTimeEnd,
                ct);

            await exportClient.AwaitExportAsync(operationId, ct);

            throw new NotImplementedException("Append URLs");
            //blockItem.ChangeState(SourceBlockState.Exported);
            //await RowItemGateway.AppendAsync(blockItem, ct);
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