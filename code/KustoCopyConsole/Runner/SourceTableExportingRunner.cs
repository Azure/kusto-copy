using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class SourceTableExportingRunner : RunnerBase
    {
        public SourceTableExportingRunner(
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
            if (!RowItemGateway.InMemoryCache
                .SourceTableMap[sourceTableRowItem.SourceTable]
                .IterationMap[sourceTableRowItem.IterationId]
                .BlockMap.ContainsKey(blockId))
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
            }

            var blockItem = RowItemGateway.InMemoryCache
                .SourceTableMap[sourceTableRowItem.SourceTable]
                .IterationMap[sourceTableRowItem.IterationId]
                .BlockMap[blockId]
                .RowItem;

            if (blockItem.State == SourceBlockState.Exporting)
            {
            }
        }
    }
}