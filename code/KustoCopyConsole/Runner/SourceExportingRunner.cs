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
                await CleanUrlsAsync(blockItem);
            }
        }

        private async Task CleanUrlsAsync(SourceBlockRowItem blockItem)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
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