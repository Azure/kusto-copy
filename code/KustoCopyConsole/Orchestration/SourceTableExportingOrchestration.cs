using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    /// <summary>
    /// This orchestration is responsible to export data to storage.
    /// It also is the component fetching the DM storage account if no storage is provided.
    /// </summary>
    internal class SourceTableExportingOrchestration : SubOrchestrationBase
    {
        #region Inner Types
        private record StageStorageCache(DateTime FetchTime, IImmutableList<Uri> StorageRoots);
        #endregion

        private static readonly TimeSpan CACHE_REFRESH_RATE = TimeSpan.FromMinutes(10);

        private readonly IDictionary<Uri, Task<StageStorageCache>> _clusterToStagingStorageRoot =
            new Dictionary<Uri, Task<StageStorageCache>>();

        public SourceTableExportingOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;

            foreach (var table in cache.SourceTableMap.Values)
            {
                foreach (var iteration in table.IterationMap.Values)
                {
                    foreach (var block in iteration.BlockMap.Values)
                    {
                        if (block.RowItem.ParseState<SourceBlockState>() == SourceBlockState.Planned)
                        {
                            BackgroundTaskContainer.AddTask(OnExportingIterationAsync(
                                block.RowItem,
                                ct));
                        }
                    }
                }
            }

            await Task.CompletedTask;
        }

        protected override void OnProcessRowItemAppended(RowItemAppend e, CancellationToken ct)
        {
            base.OnProcessRowItemAppended(e, ct);

            if (e.Item.RowType == RowType.SourceTable
                && e.Item.ParseState<SourceTableState>() == SourceTableState.Planned)
            {
                BackgroundTaskContainer.AddTask(OnExportingIterationAsync(
                    e.Item,
                    ct));
            }
        }

        private async Task OnExportingIterationAsync(RowItem item, CancellationToken ct)
        {
            if (Parameterization.StorageUrls.Any())
            {
                throw new NotImplementedException(
                    "Support for storage accounts not supported yet");
            }
            else
            {
                var sourceTableId = item.GetSourceTableIdentity();
                var activity = Parameterization.Activities
                    .Where(a => a.Source.GetTableIdentity() == sourceTableId)
                    .FirstOrDefault();

                if (activity == null)
                {
                    throw new CopyException(
                        $"Table {sourceTableId} present in transaction log but not in "
                        + $"configuration",
                        false);
                }
                if (activity.Destinations.Count() != 1)
                {
                    throw new CopyException(
                        $"Table {sourceTableId} expected to have 1 destination but has"
                        + $" {activity.Destinations.Count()}",
                        false);
                }

                var destinationTableId = activity.Destinations.First().GetTableIdentity();
                var storageRoots =
                    await GetCachedStorageRootsAsync(destinationTableId.ClusterUri);
            }
            throw new NotImplementedException();
        }

        private async Task<IImmutableList<Uri>> GetCachedStorageRootsAsync(Uri clusterUri)
        {
            Task<StageStorageCache> storageCacheTask;

            lock (_clusterToStagingStorageRoot)
            {
                if (!_clusterToStagingStorageRoot.ContainsKey(clusterUri))
                {
                    _clusterToStagingStorageRoot.Add(clusterUri, FetchStageStorageCacheAsync(clusterUri));
                }
                storageCacheTask = _clusterToStagingStorageRoot[clusterUri];
            }

            var storageCache = await storageCacheTask;

            if (DateTime.Now.Subtract(storageCache.FetchTime) > CACHE_REFRESH_RATE)
            {
                lock (_clusterToStagingStorageRoot)
                {
                    if (object.ReferenceEquals(
                        storageCacheTask,
                        _clusterToStagingStorageRoot[clusterUri]))
                    {
                        _clusterToStagingStorageRoot[clusterUri] =
                            FetchStageStorageCacheAsync(clusterUri);
                    }
                }

                return await GetCachedStorageRootsAsync(clusterUri);
            }
            else
            {
                return storageCache.StorageRoots;
            }
        }

        private Task<StageStorageCache> FetchStageStorageCacheAsync(Uri clusterUri)
        {
        }
    }
}