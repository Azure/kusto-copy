using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : RunnerBase
    {
        #region Inner Types
        private record CapacityCache(DateTime CachedTime, int CachedCapacity);

        private record ExportLineUp(Uri ClusterUri, BlockRowItem BlockItem);
        #endregion

        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(5);

        public ExportingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var capacityMap = new Dictionary<Uri, CapacityCache>();

            while (!AllActivitiesCompleted())
            {
                var exportCount = await StartExportAsync(capacityMap, ct);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task<int> StartExportAsync(
            IDictionary<Uri, CapacityCache> capacityMap,
            CancellationToken ct)
        {
            var exportLineUp = await GetExportLineUpAsync(capacityMap, ct);
            var enrichedLineUp = exportLineUp
                .Select(o => new
                {
                    DbClient = DbClientFactory.GetDbCommandClient(o.ClusterUri, string.Empty),
                    Query = Parameterization
                    .Activities[o.BlockItem.ActivityName]
                    .KqlQuery,
                    IterationItem = RowItemGateway.InMemoryCache
                    .ActivityMap[o.BlockItem.ActivityName]
                    .IterationMap[o.BlockItem.IterationId]
                    .RowItem,
                    o.BlockItem
                });
            var list = new List<(Task<string> Task, BlockRowItem BlockItem)>();

            foreach (var lineUpItem in enrichedLineUp)
            {
                var folderPath = $"iterations/{lineUpItem.IterationItem.IterationId:D20}" +
                    $"/blocks/{lineUpItem.BlockItem.BlockId:D20}";
                var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                    folderPath, ct);
                var task = lineUpItem.DbClient.ExportBlockAsync(
                    new KustoPriority(lineUpItem.BlockItem.GetIterationKey()),
                    writableUris,
                    lineUpItem.Query,
                    lineUpItem.IterationItem.CursorStart,
                    lineUpItem.IterationItem.CursorEnd,
                    lineUpItem.BlockItem.IngestionTimeStart,
                    lineUpItem.BlockItem.IngestionTimeEnd,
                    ct);

                list.Add((task, lineUpItem.BlockItem));
            }

            await Task.WhenAll(list.Select(o => o.Task));

            foreach (var exportItem in list)
            {
                var blockItem = exportItem.BlockItem.ChangeState(BlockState.Exporting);

                blockItem.OperationId = exportItem.Task.Result;
                RowItemGateway.Append(blockItem);
            }

            return list.Count;
        }

        private async Task<IEnumerable<ExportLineUp>> GetExportLineUpAsync(
            IDictionary<Uri, CapacityCache> capacityMap,
            CancellationToken ct)
        {
            //  Ensure we have the capacity for clusters with candidates 
            await EnsureCapacityCache(capacityMap, ct);

            var allBlocks = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed)
                .SelectMany(i => i.BlockMap.Values)
                .Select(b => b.RowItem)
                .Where(b => b.State == BlockState.Exporting || b.State == BlockState.Planned);
            var status = allBlocks
                .Select(b => new
                {
                    BlockItem = b,
                    RowItemGateway.InMemoryCache.ActivityMap[b.ActivityName]
                    .RowItem.SourceTable.ClusterUri
                })
                .GroupBy(o => o.ClusterUri, o => o.BlockItem)
                .Where(g => g.Any(b => b.State == BlockState.Planned))
                .Select(g => new
                {
                    ClusterUri = g.Key,
                    Capacity = capacityMap[g.Key].CachedCapacity,
                    ExportingCount = g.Count(b => b.State == BlockState.Exporting),
                    Candidates = g
                    .Where(b => b.State == BlockState.Planned)
                    .OrderBy(b => new KustoPriority(b.GetIterationKey()))
                });
            var exportLineUp = status
                .Where(o => o.Capacity > o.ExportingCount)
                .SelectMany(o => o.Candidates.Take(o.Capacity - o.ExportingCount).Select(
                    b => new ExportLineUp(o.ClusterUri, b)));

            return exportLineUp;
        }

        private async Task EnsureCapacityCache(
            IDictionary<Uri, CapacityCache> capacityMap,
            CancellationToken ct)
        {
            var clustersToUpdate = Parameterization
                .Activities
                .Values
                .Select(a => a.Source.GetTableIdentity().ClusterUri)
                .Distinct()
                .Where(u => !capacityMap.ContainsKey(u)
                || capacityMap[u].CachedTime + CAPACITY_REFRESH_PERIOD < DateTime.Now);
            var capacityUpdateTasks = clustersToUpdate
                .Select(u => new
                {
                    ClusterUri = u,
                    CapacityTask = FetchCapacityAsync(u, ct)
                })
                .ToImmutableArray();

            await Task.WhenAll(capacityUpdateTasks.Select(o => o.CapacityTask));

            foreach (var update in capacityUpdateTasks)
            {
                capacityMap[update.ClusterUri] =
                    new CapacityCache(DateTime.Now, update.CapacityTask.Result);
            }
        }

        private async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowExportCapacityAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }

        private BlockRowItem CleanUrls(BlockRowItem blockItem)
        {
            var existingUrls = RowItemGateway.InMemoryCache
                .ActivityMap[blockItem.ActivityName]
                .IterationMap[blockItem.IterationId]
                .BlockMap[blockItem.BlockId]
                .UrlMap
                .Values;

            foreach (var url in existingUrls)
            {
                RowItemGateway.Append(url.RowItem.ChangeState(UrlState.Deleted));
            }
            blockItem = blockItem.ChangeState(BlockState.Exporting);
            RowItemGateway.Append(blockItem);

            return blockItem;
        }
    }
}