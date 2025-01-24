using Azure.Core;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentsRunner : RunnerBase
    {
        public MoveExtentsRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                    a => a.RowItem.State != ActivityState.Completed,
                    i => i.RowItem.State != IterationState.Completed);
                var ingestedBlocks = allBlocks
                    .Where(h => h.Block.State == BlockState.Ingested);
                var moveTasks = ingestedBlocks
                    .Select(h => UpdateIngestedBlockAsync(h, ct))
                    .ToImmutableArray();

                await Task.WhenAll(moveTasks);

                if (!moveTasks.Any())
                {
                    //  Sleep
                    await SleepAsync(ct);
                }
            }
        }

        private async Task UpdateIngestedBlockAsync(ActivityFlatHierarchy item, CancellationToken ct)
        {
            var commandClient = DbClientFactory.GetDbCommandClient(
                item.Activity.DestinationTable.ClusterUri,
                item.Activity.DestinationTable.DatabaseName);
            var priority = new KustoPriority(item.Block.GetBlockKey());
            var extentCount = await commandClient.MoveExtentsAsync(
                priority,
                item.TempTable!.TempTableName,
                item.Activity.DestinationTable.TableName,
                item.Block.BlockTag,
                ct);
            var cleanCount = await commandClient.CleanExtentTagsAsync(
                priority,
                item.Activity.DestinationTable.TableName,
                item.Block.BlockTag,
                ct);
            var newBlockItem = item.Block.ChangeState(BlockState.ExtentMoved);

            RowItemGateway.Append(newBlockItem);
        }
    }
}