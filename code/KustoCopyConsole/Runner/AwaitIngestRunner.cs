using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class AwaitIngestRunner : RunnerBase
    {
        private static readonly TimeSpan REFRESH_PERIOD = TimeSpan.FromSeconds(5);

        public AwaitIngestRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(BlockRowItem blockItem, CancellationToken ct)
        {
            if (blockItem.State == BlockState.Queued)
            {
                blockItem = await AwaitIngestionAsync(blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> AwaitIngestionAsync(
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            var iterationCache = RowItemGateway.InMemoryCache
                .SourceTableMap[blockItem.SourceTable]
                .IterationMap[blockItem.IterationId];
            var tempTableName = iterationCache.RowItem.TempTableName;
            var targetRowCount = iterationCache.BlockMap[blockItem.BlockId].UrlMap.Values
                .Sum(u => u.RowItem.RowCount);
            var commandClient = DbClientFactory.GetDbCommandClient(
                blockItem.DestinationTable.ClusterUri,
                blockItem.DestinationTable.DatabaseName);

            while (true)
            {
                var rowCount = await commandClient.GetExtentRowCountAsync(
                    blockItem.IterationId,
                    blockItem.DestinationTable.TableName,
                    tempTableName,
                    blockItem.BlockTag,
                    ct);

                if (rowCount > targetRowCount)
                {
                    throw new CopyException(
                        $"Target row count is {targetRowCount} while we observe {rowCount}",
                        false);
                }
                if (rowCount == targetRowCount)
                {
                    blockItem = blockItem.ChangeState(BlockState.Ingested);
                    await RowItemGateway.AppendAsync(blockItem, ct);

                    return blockItem;
                }
                else
                {
                    await Task.Delay(REFRESH_PERIOD, ct);
                }
            }
        }
    }
}