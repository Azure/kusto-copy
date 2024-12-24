using Azure.Data.Tables;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class SourceTablePlanningRunner : RunnerBase
    {
        private const long MAX_RECORDS_PER_BLOCK = 1048576;

        public SourceTablePlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task RunAsync(SourceTableRowItem sourceTableRowItem, CancellationToken ct)
        {
            if (sourceTableRowItem.State == SourceTableState.Planning)
            {
                var blockMap = RowItemGateway.InMemoryCache
                    .SourceTableMap[sourceTableRowItem.SourceTable]
                    .IterationMap[sourceTableRowItem.IterationId]
                    .BlockMap;
                var lastBlockItem = blockMap.Any()
                    ? blockMap.Values.Select(i => i.RowItem).ArgMax(b => b.BlockId)
                    : null;

                while (true)
                {
                    lastBlockItem = await PlanNewBlockAsync(
                        sourceTableRowItem,
                        lastBlockItem,
                        ct);
                }
            }
        }

        private async Task<SourceBlockRowItem?> PlanNewBlockAsync(
            SourceTableRowItem sourceTableItem,
            SourceBlockRowItem? lastBlockItem,
            CancellationToken ct)
        {
            var ingestionTimeStart = lastBlockItem == null
                ? string.Empty
                : lastBlockItem.IngestionTimeEnd;
            var queryClient = DbClientFactory.GetDbQueryClient(
                sourceTableItem.SourceTable.ClusterUri,
                sourceTableItem.SourceTable.DatabaseName);
            var timeResolutionInSeconds = 100000;
            var timeResolution = TimeSpan.FromSeconds(timeResolutionInSeconds);
            var distributions = await queryClient.GetRecordDistributionAsync(
                sourceTableItem.IterationId,
                sourceTableItem.SourceTable.TableName,
                sourceTableItem.CursorStart,
                sourceTableItem.CursorEnd,
                ingestionTimeStart,
                timeResolution,
                ct);

            throw new NotImplementedException();
        }
    }
}