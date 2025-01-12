using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentsRunner : RunnerBase
    {
        public MoveExtentsRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(BlockRowItem blockItem, CancellationToken ct)
        {
            if (blockItem.State == BlockState.Ingested)
            {
                blockItem = await MoveExtentsAsync(blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> MoveExtentsAsync(
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            if (blockItem.State == BlockState.Ingested)
            {
                var tableItem = RowItemGateway.InMemoryCache
                    .SourceTableMap[blockItem.SourceTable]
                    .IterationMap[blockItem.IterationId]
                    .RowItem;
                var commandClient = DbClientFactory.GetDbCommandClient(
                    blockItem.DestinationTable.ClusterUri,
                    blockItem.DestinationTable.DatabaseName);
                var extentCount = await commandClient.MoveExtentsAsync(
                    blockItem.IterationId,
                    tableItem.TempTableName,
                    blockItem.DestinationTable.TableName,
                    blockItem.BlockTag,
                    ct);
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    blockItem.IterationId,
                    blockItem.DestinationTable.TableName,
                    blockItem.BlockTag,
                    ct);

                blockItem = blockItem.ChangeState(BlockState.ExtentMoved);
                await RowItemGateway.AppendAsync(blockItem, ct);
            }

            return blockItem;
        }
    }
}