using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentRunner : ActivityRunnerBase
    {
        private const int MAXIMUM_EXTENT_MOVING = 100;

        public MoveExtentRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(15))
        {
        }

        public override async Task RunActivityAsync(string activityName, CancellationToken ct)
        {
            while (!IsActivityCompleted(activityName))
            {
                await MoveAsync(ct);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task MoveAsync(CancellationToken ct)
        {
            var allBlocks = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var ingestedBlocks = allBlocks
                .Where(h => h.Block.State == BlockState.Ingested);
            var moveTasks = ingestedBlocks
                .GroupBy(h => h.Iteration.GetIterationKey())
                .Select(g => MoveBlocksFromIterationAsync(
                    g.First().Activity,
                    g.First().Iteration,
                    g.First().TempTable!.TempTableName,
                    g,
                    ct))
                .ToImmutableArray();

            Trace.TraceInformation($"AwaitIngest:  {moveTasks.Count()} extent moving commands");
            await TaskHelper.WhenAllWithErrors(moveTasks);
        }

        private async Task MoveBlocksFromIterationAsync(
            ActivityRowItem activity,
            IterationRowItem iteration,
            string tempTableName,
            IEnumerable<ActivityFlatHierarchy> items,
            CancellationToken ct)
        {
            var commandClient = DbClientFactory.GetDbCommandClient(
                activity.DestinationTable.ClusterUri,
                activity.DestinationTable.DatabaseName);
            var priority = new KustoPriority(iteration.GetIterationKey());
            var sortedItems = items
                .OrderBy(i => i.Block.BlockId)
                .ToImmutableArray();

            while (sortedItems.Any())
            {
                var movingItems = TakeMovingBlocks(sortedItems);
                var movingExtentIds = movingItems
                    .SelectMany(i => i.Extents.Select(e => e.ExtentId));
                var tags = movingItems
                    .Select(i => i.Block.BlockTag);
                var extentCount = await commandClient.MoveExtentsAsync(
                    priority,
                    tempTableName,
                    activity.DestinationTable.TableName,
                    movingExtentIds,
                    ct);
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    priority,
                    activity.DestinationTable.TableName,
                    tags,
                    ct);
                var newBlockItems = movingItems
                    .Select(i => i.Block.ChangeState(BlockState.ExtentMoved))
                    .ToImmutableArray();

                foreach (var item in newBlockItems)
                {
                    item.BlockTag = string.Empty;
                }
                RowItemGateway.Append(newBlockItems);
                sortedItems = sortedItems
                    .Skip(movingItems.Count())
                    .ToImmutableArray();
            }
        }

        private IEnumerable<ActivityFlatHierarchy> TakeMovingBlocks(
            IEnumerable<ActivityFlatHierarchy> items)
        {
            var i = 0;
            var totalExtents = 0;

            foreach (var item in items)
            {
                if (totalExtents + item.Extents.Count() > MAXIMUM_EXTENT_MOVING)
                {
                    return items.Take(Math.Max(1, i));
                }
                else
                {
                    totalExtents += item.Extents.Count();
                    ++i;
                }
            }

            return items;
        }
    }
}