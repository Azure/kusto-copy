using Azure.Core;
using Azure.Storage.Blobs.Models;
using Kusto.Data;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    /// <summary>
    /// This orchestration is responsible to export data to storage.
    /// It plays a similar role to the DM (importer) but for exporting.
    /// It also is the component fetching the DM storage account if no storage is provided.
    /// </summary>
    internal class SourceTableExportingOrchestration : SubOrchestrationBase
    {
        #region Inner Types
        private record ClusterCache(
            DateTime FetchTime,
            IImmutableList<Uri> ExportRootUris,
            int ExportCapacity);

        private class ClusterQueue
        {
            private readonly object _lock = new object();
            private readonly Func<CancellationToken, Task<ClusterCache>> _clusterCacheFetch;
            private Task<ClusterCache>? _cacheTask;

            public ClusterQueue(
                PriorityQueue<RowItem, KustoPriority> exportQueue,
                IDictionary<string, RowItem> operationIdToBlockMap,
                Func<CancellationToken, Task<ClusterCache>> clusterCacheFetch)
            {
                ExportQueue = exportQueue;
                OperationIdToBlockMap = operationIdToBlockMap;
                _clusterCacheFetch = clusterCacheFetch;
            }

            public PriorityQueue<RowItem, KustoPriority> ExportQueue { get; }

            public IDictionary<string, RowItem> OperationIdToBlockMap { get; }

            public async Task<ClusterCache> GetClusterCacheAsync(CancellationToken ct)
            {
                var cacheTask = _cacheTask;

                if (cacheTask == null
                    || DateTime.Now.Subtract((await cacheTask).FetchTime) > CACHE_REFRESH_RATE)
                {
                    lock (_lock)
                    {   //  Double checked it wasn't fetched in the meantime
                        if (object.ReferenceEquals(cacheTask, _cacheTask))
                        {
                            _cacheTask = _clusterCacheFetch(ct);
                        }
                    }
                }

                return await _cacheTask!;
            }
        }
        #endregion

        private static readonly TimeSpan CACHE_REFRESH_RATE = TimeSpan.FromMinutes(10);

        private readonly IDictionary<Uri, ClusterQueue> _clusterToExportQueue =
            new Dictionary<Uri, ClusterQueue>();

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
                        if (block.RowItem.ParseState<SourceBlockState>()
                            == SourceBlockState.Planned
                            || block.RowItem.ParseState<SourceBlockState>()
                            == SourceBlockState.Exporting)
                        {
                            BackgroundTaskContainer.AddTask(OnQueueExportingItemAsync(
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

            if (e.Item.RowType == RowType.SourceBlock
                && e.Item.ParseState<SourceBlockState>() == SourceBlockState.Planned)
            {
                BackgroundTaskContainer.AddTask(OnQueueExportingItemAsync(
                    e.Item,
                    ct));
            }
        }

        private async Task OnQueueExportingItemAsync(RowItem blockItem, CancellationToken ct)
        {
            var tableIdentity = blockItem.GetSourceTableIdentity();
            var clusterQueue = GetClusterQueue(blockItem, ct);

            clusterQueue.ExportQueue.Enqueue(
                blockItem,
                new KustoPriority(
                    tableIdentity.DatabaseName,
                    new KustoDbPriority(
                        blockItem.IterationId,
                        tableIdentity.TableName,
                        blockItem.BlockId)));

            if (blockItem.ParseState<SourceBlockState>() == SourceBlockState.Planned)
            {
                var newBlockItem = blockItem.Clone();

                newBlockItem.State = SourceBlockState.Exporting.ToString();

                await RowItemGateway.AppendAsync(newBlockItem, ct);
            }
        }

        #region Cluster queue
        private ClusterQueue GetClusterQueue(RowItem blockItem, CancellationToken ct)
        {
            var tableIdentity = blockItem.GetSourceTableIdentity();

            lock (_clusterToExportQueue)
            {
                if (!_clusterToExportQueue.ContainsKey(tableIdentity.ClusterUri))
                {
                    _clusterToExportQueue[tableIdentity.ClusterUri] = new ClusterQueue(
                        new PriorityQueue<RowItem, KustoPriority>(),
                        new Dictionary<string, RowItem>(),
                        ctt => FetchClusterCacheAsync(blockItem, ctt));
                }

                return _clusterToExportQueue[tableIdentity.ClusterUri];
            }
        }

        #region Fetch
        private async Task<ClusterCache> FetchClusterCacheAsync(
            RowItem blockItem,
            CancellationToken ct)
        {
            var exportRootUrisTask = FetchExportRootUrisAsync(blockItem, ct);
            var exportCapacity = await FetchExportCapacityAsync(blockItem, ct);
            var exportRootUris = await exportRootUrisTask;

            return new ClusterCache(DateTime.Now, exportRootUris, exportCapacity);
        }

        private Task<int> FetchExportCapacityAsync(RowItem blockItem, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        private async Task<IImmutableList<Uri>> FetchExportRootUrisAsync(
            RowItem blockItem,
            CancellationToken ct)
        {
            var sourceTableId = blockItem.GetSourceTableIdentity();

            if (Parameterization.StorageUrls.Any())
            {
                throw new NotImplementedException(
                    "Support for storage accounts not supported yet");
            }
            else
            {
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
                var dmCommandClient = DbClientFactory.GetDmCommandClient(
                    destinationTableId.ClusterUri,
                    destinationTableId.DatabaseName);
                var tempStorageUris = await dmCommandClient.GetTempStorageUrisAsync(ct);

                return tempStorageUris;
            }
        }
        #endregion
        #endregion
    }
}