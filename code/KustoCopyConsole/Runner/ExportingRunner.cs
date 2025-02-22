using Azure.Core;
using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;
using YamlDotNet.Core.Tokens;

namespace KustoCopyConsole.Runner
{
    internal class ExportingRunner : RunnerBase
    {
        #region Inner Types
        private record CapacityCache(DateTime CachedTime, int CachedCapacity);
        #endregion

        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(5);

        public ExportingRunner(
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
                 TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var capacityMap = new Dictionary<Uri, CapacityCache>();

            while (!AllActivitiesCompleted())
            {
                var candidatesByCluster = GetCandidatesByCluster();

                //  Ensure (or fetch) capacity of each cluster having candidates
                await EnsureCapacityCacheAsync(capacityMap, candidatesByCluster.Keys, ct);

                var processTasks = candidatesByCluster
                    .Select(p => Task.Run(() => ProcessExportByClusterAsync(
                        p.Key,
                        capacityMap[p.Key].CachedCapacity,
                        p.Value,
                        ct)))
                    .ToImmutableArray();

                await Task.WhenAll(processTasks);
                await SleepAsync(ct);
            }
        }

        private IImmutableDictionary<Uri, IImmutableDictionary<IterationKey, IImmutableList<BlockRowItem>>>
            GetCandidatesByCluster()
        {
            IImmutableDictionary<IterationKey, IImmutableList<BlockRowItem>> GroupByIteration(
                IEnumerable<ActivityFlatHierarchy> flatHierarchy)
            {
                var map = flatHierarchy
                    .GroupBy(
                        h => new IterationKey(h.Activity.ActivityName, h.Iteration.IterationId),
                        h => h.Block)
                    .ToImmutableDictionary(
                        g => g.Key,
                        g => (IImmutableList<BlockRowItem>)g.ToImmutableArray());

                return map;
            }
            var flatHierarchy = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            var plannedOnly = flatHierarchy
                .Where(h => h.Block.State == BlockState.Planned);
            var candidates = plannedOnly
                .GroupBy(h => h.Activity.SourceTable.ClusterUri)
                .ToImmutableDictionary(
                    g => g.Key,
                    g => GroupByIteration(g));

            return candidates;
        }

        private async Task EnsureCapacityCacheAsync(
            IDictionary<Uri, CapacityCache> capacityMap,
            IEnumerable<Uri> clusterUris,
            CancellationToken ct)
        {
            var clustersToUpdate = clusterUris
                .Where(u => !capacityMap.ContainsKey(u)
                || capacityMap[u].CachedTime + CAPACITY_REFRESH_PERIOD < DateTime.Now);
            var capacityUpdateTasks = clustersToUpdate
                .Select(u => new
                {
                    ClusterUri = u,
                    CapacityTask = FetchCapacityAsync(u, ct)
                })
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(capacityUpdateTasks.Select(o => o.CapacityTask));

            foreach (var update in capacityUpdateTasks)
            {
                capacityMap[update.ClusterUri] =
                    new CapacityCache(DateTime.Now, update.CapacityTask.Result);
            }
        }

        private async Task<int> ProcessExportByClusterAsync(
            Uri clusterUri,
            int capacity,
            IImmutableDictionary<IterationKey, IImmutableList<BlockRowItem>> iterationMap,
            CancellationToken ct)
        {
            var exportingCount = GetExportingCount(clusterUri);
            var freeCapacity = capacity - exportingCount;
            var orderedIterationKeys = iterationMap.Keys
                .OrderBy(k => k.ActivityName)
                .ThenBy(k => k.IterationId);
            var lineup = new List<BlockRowItem>();

            //  Get line up by iteration priority
            foreach (var iterationKey in orderedIterationKeys)
            {
                if (freeCapacity > 0)
                {
                    var candidates = iterationMap[iterationKey];

                    freeCapacity -= lineup.Count();
                    lineup.AddRange(GetLineup(iterationKey, candidates, freeCapacity));
                }
            }

            var startExportTasks = lineup
                .Select(b => Task.Run(() => StartExportAsync(b, ct)))
                .ToImmutableArray();

            await Task.WhenAll(startExportTasks);

            return lineup.Count;
        }

        private int GetExportingCount(Uri clusterUri)
        {
            var flatHierarchy = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.SourceTable.ClusterUri == clusterUri,
                i => i.RowItem.State != IterationState.Completed);
            var exportCount = flatHierarchy
                .Where(h => h.Block.State == BlockState.Exporting)
                .Count();

            return exportCount;
        }

        private IEnumerable<BlockRowItem> GetLineup(
            IterationKey iterationKey,
            IEnumerable<BlockRowItem> candidates,
            int freeCapacity)
        {
            const int MIN_STAT_COUNT = 5;
            const long MIN_ROW_COUNT_STATS = 100000;
            const long MAX_ROW_COUNT = 16000000;
            const int LEAP_RATIO = 3;
            var MAX_EXPORT_DURATION = TimeSpan.FromMinutes(1);

            var latestBlocks = RowItemGateway.InMemoryCache
                .ActivityMap[iterationKey.ActivityName]
                .IterationMap[iterationKey.IterationId]
                .BlockMap
                .Values
                .Select(c => c.RowItem)
                .Where(b => b.State >= BlockState.Exported)
                //  We want representative export, i.e. meaningful size
                .Where(b => b.ExportedRowCount > MIN_ROW_COUNT_STATS)
                .OrderByDescending(b => b.Updated)
                .Take(MIN_STAT_COUNT)
                .ToImmutableArray();

            if (latestBlocks.Length == MIN_STAT_COUNT)
            {   //  Replan blocks
                var totalDuration = latestBlocks.Sum(b => b.ExportDuration!.Value.TotalSeconds);
                var totalRows = latestBlocks.Sum(b => b.ExportedRowCount);
                var maxRowCount = latestBlocks.Max(b => b.ExportedRowCount);
                var averageDurationPerRow = TimeSpan.FromSeconds(totalDuration / totalRows);
                var targetRowCount = Math.Max(
                    1,
                    Math.Min(
                        Math.Min(MAX_ROW_COUNT, LEAP_RATIO * maxRowCount),
                        MAX_EXPORT_DURATION / averageDurationPerRow));

                return GetReplannedLineup(candidates, targetRowCount, freeCapacity);
            }
            else
            {   //  Just return the top blocks
                return candidates
                    .OrderBy(b => b.IngestionTimeStart)
                    .Take(freeCapacity)
                    .ToImmutableArray();
            }
        }

        private IEnumerable<BlockRowItem> GetReplannedLineup(
            IEnumerable<BlockRowItem> candidates,
            double targetRowCount,
            int freeCapacity)
        {
            //  Stack them upside down
            var candidateStack =
                new Stack<BlockRowItem>(candidates.OrderByDescending(c => c.IngestionTimeStart));
            var lineup = new List<BlockRowItem>(freeCapacity);

            while (freeCapacity - lineup.Count > 0 && candidateStack.Any())
            {
                var first = candidateStack.Pop();

                if (candidateStack.Any())
                {
                    var second = candidateStack.Pop();
                    var merge = new BlockRowItem
                    {
                        State = first.State,
                        ActivityName = first.ActivityName,
                        IterationId = first.IterationId,
                        BlockId = first.BlockId,
                        IngestionTimeStart = first.IngestionTimeStart,
                        IngestionTimeEnd = second.IngestionTimeEnd,
                        MinCreationTime = first.MinCreationTime < second.MinCreationTime
                        ? first.MinCreationTime
                        : second.MinCreationTime,
                        MaxCreationTime = first.MaxCreationTime > second.MaxCreationTime
                        ? first.MaxCreationTime
                        : second.MaxCreationTime,
                        PlannedRowCount = first.PlannedRowCount + second.PlannedRowCount,
                        ReplannedBlockIds = first.ReplannedBlockIds
                        .Concat(second.ReplannedBlockIds)
                        .Append(second.BlockId)
                        .ToImmutableArray(),
                    };

                    if (merge.PlannedRowCount <= targetRowCount
                        && (merge.MaxCreationTime - merge.MinCreationTime < TimeSpan.FromDays(1)))
                    {
                        candidateStack.Push(merge);
                    }
                    else
                    {
                        lineup.Add(first);
                        candidateStack.Push(second);
                    }
                }
                else
                {
                    lineup.Add(first);
                }
            }

            return lineup;
        }

        private async Task StartExportAsync(BlockRowItem item, CancellationToken ct)
        {
            var activity = RowItemGateway.InMemoryCache.ActivityMap[item.ActivityName].RowItem;
            var iteration = RowItemGateway.InMemoryCache
                .ActivityMap[item.ActivityName]
                .IterationMap[item.IterationId]
                .RowItem;
            var dbClient = DbClientFactory.GetDbCommandClient(
                activity.SourceTable.ClusterUri,
                activity.SourceTable.DatabaseName);
            var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                item.GetBlockKey(),
                ct);
            var query = Parameterization.Activities[item.ActivityName].KqlQuery;
            var operationId = await dbClient.ExportBlockAsync(
                new KustoPriority(item.GetBlockKey()),
                writableUris,
                activity.SourceTable.TableName,
                query,
                iteration.CursorStart,
                iteration.CursorEnd,
                item.IngestionTimeStart,
                item.IngestionTimeEnd,
                ct);
            var newBlockItem = item.ChangeState(BlockState.Exporting);

            newBlockItem.ExportOperationId = operationId;
            RowItemGateway.Append(newBlockItem);
        }

        private async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowExportCapacityAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }
    }
}