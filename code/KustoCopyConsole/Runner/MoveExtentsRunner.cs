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
                var activityCache = RowItemGateway.InMemoryCache
                    .ActivityMap[blockItem.ActivityName];
                var iterationItem = activityCache
                    .IterationMap[blockItem.IterationId]
                    .RowItem;
                var commandClient = DbClientFactory.GetDbCommandClient(
                    activityCache.RowItem.DestinationTable.ClusterUri,
                    activityCache.RowItem.DestinationTable.DatabaseName);
                var priority = new KustoPriority(
                    blockItem.ActivityName, blockItem.IterationId, blockItem.BlockId);
                var extentCount = await commandClient.MoveExtentsAsync(
                    priority,
                    iterationItem.TempTableName,
                    activityCache.RowItem.DestinationTable.TableName,
                    blockItem.BlockTag,
                    ct);
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    priority,
                    activityCache.RowItem.DestinationTable.TableName,
                    blockItem.BlockTag,
                    ct);

                blockItem = blockItem.ChangeState(BlockState.ExtentMoved);
                await RowItemGateway.AppendAsync(blockItem, ct);
            }

            return blockItem;
        }
    }
}