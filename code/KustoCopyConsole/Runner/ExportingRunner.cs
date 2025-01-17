﻿using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using Microsoft.Extensions.Azure;
using System;
using System.Collections.Immutable;
using System.Linq;

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
                var exportLineUp = await GetExportLineUpAsync(capacityMap, ct);
                var exportCount = await StartExportAsync(exportLineUp, ct);

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task<int> StartExportAsync(
            IEnumerable<ActivityFlatHierarchy> exportLineUp,
            CancellationToken ct)
        {
            async Task ProcessBlockAsync(
                ActivityFlatHierarchy item,
                CancellationToken ct)
            {
                var dbClient = DbClientFactory.GetDbCommandClient(
                    item.Activity.SourceTable.ClusterUri,
                    item.Activity.SourceTable.DatabaseName);
                var folderPath = $"activities/{item.Activity.ActivityName}" +
                    $"iterations/{item.Iteration.IterationId:D20}" +
                    $"/blocks/{item.BlockItem.BlockId:D20}";
                var writableUris = await StagingBlobUriProvider.GetWritableFolderUrisAsync(
                    folderPath,
                    ct);
                var query = Parameterization.Activities[item.Activity.ActivityName].KqlQuery;
                var operationId = await dbClient.ExportBlockAsync(
                    new KustoPriority(item.BlockItem.GetIterationKey()),
                    writableUris,
                    query,
                    item.Iteration.CursorStart,
                    item.Iteration.CursorEnd,
                    item.BlockItem.IngestionTimeStart,
                    item.BlockItem.IngestionTimeEnd,
                    ct);
                var blockItem = item.BlockItem.ChangeState(BlockState.Exporting);

                blockItem.OperationId = operationId;
                RowItemGateway.Append(blockItem);
            }

            var tasks = exportLineUp
                .Select(h => ProcessBlockAsync(h, ct))
                .ToImmutableArray();

            await Task.WhenAll(tasks);

            return tasks.Count();
        }

        private async Task<IEnumerable<ActivityFlatHierarchy>> GetExportLineUpAsync(
            IDictionary<Uri, CapacityCache> capacityMap,
            CancellationToken ct)
        {
            var flatHierarchy = RowItemGateway.InMemoryCache.GetActivityFlatHierarchy(
                a => a.RowItem.State != ActivityState.Completed,
                i => i.RowItem.State != IterationState.Completed);
            //  Find candidates for export and group them by cluster
            var candidatesByCluster = flatHierarchy
                .GroupBy(h => h.Activity.SourceTable.ClusterUri)
                .Select(g => new
                {
                    ClusterUri = g.Key,
                    ExportingCount = g.Count(h => h.BlockItem.State == BlockState.Exporting),
                    Candidates = g.Where(h => h.BlockItem.State == BlockState.Planned)
                })
                //  Keep only clusters with candidates
                .Where(o => o.Candidates.Any())
                .ToImmutableDictionary(o => o.ClusterUri);

            //  Ensure we have the capacity for clusters with candidates 
            await EnsureCapacityCache(capacityMap, candidatesByCluster.Keys, ct);

            //  Create the line up
            var exportLineUp = candidatesByCluster
                .Values
                .Select(o => new
                {
                    o.Candidates,
                    //  Find the export availability (capacity - current usage)
                    ExportingAvailability = capacityMap[o.ClusterUri].CachedCapacity - o.ExportingCount
                })
                //  Keep only clusters with availability
                .Where(o => o.ExportingAvailability > 0)
                //  Select candidates by priority
                .SelectMany(o => o.Candidates
                .OrderBy(c => new KustoPriority(c.BlockItem.GetIterationKey()))
                .Take(o.ExportingAvailability))
                .ToImmutableArray();

            return exportLineUp;
        }

        private async Task EnsureCapacityCache(
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