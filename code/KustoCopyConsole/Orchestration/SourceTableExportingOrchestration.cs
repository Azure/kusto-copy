using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
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
            private readonly PriorityQueue<RowItem, KustoPriority> _exportQueue;
            private readonly IDictionary<string, RowItem> _operationIdToBlockMap;
            private readonly Func<CancellationToken, Task<ClusterCache>> _clusterCacheFetch;
            private Task<ClusterCache>? _cacheTask;

            public ClusterQueue(
                PriorityQueue<RowItem, KustoPriority> exportQueue,
                IDictionary<string, RowItem> operationIdToBlockMap,
                Func<CancellationToken, Task<ClusterCache>> clusterCacheFetch)
            {
                _exportQueue = exportQueue;
                _operationIdToBlockMap = operationIdToBlockMap;
                _clusterCacheFetch = clusterCacheFetch;
            }

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

            #region Export queue
            public bool AnyExport()
            {
                lock (_exportQueue)
                {
                    return _exportQueue.Count != 0;
                }
            }

            public void EnqueueExport(RowItem blockItem, KustoPriority kustoPriority)
            {
                lock (_exportQueue)
                {
                    _exportQueue.Enqueue(blockItem, kustoPriority);
                }
            }

            public bool TryPeekExport([MaybeNullWhen(false)] out RowItem blockItem)
            {
                lock (_exportQueue)
                {
                    return _exportQueue.TryPeek(out blockItem, out _);
                }
            }

            public void Dequeue()
            {
                lock (_exportQueue)
                {
                    _exportQueue.Dequeue();
                }
            }
            #endregion

            #region Operation ID map
            public int GetOperationIDCount()
            {
                lock (_operationIdToBlockMap)
                {
                    return _operationIdToBlockMap.Count;
                }
            }

            public void AddOperationId(string operationId, RowItem item)
            {
                _operationIdToBlockMap.Add(operationId, item);
            }
            #endregion
        }

        private class ClusterQueueCollection
        {
            private readonly MainJobParameterization _parameterization;
            private readonly DbClientFactory _dbClientFactory;
            private readonly IDictionary<Uri, ClusterQueue> _clusterToExportQueue =
                new Dictionary<Uri, ClusterQueue>();

            public ClusterQueueCollection(
                MainJobParameterization parameterization,
                DbClientFactory dbClientFactory)
            {
                _parameterization = parameterization;
                _dbClientFactory = dbClientFactory;
            }

            public bool AnyExport()
            {
                lock (_clusterToExportQueue)
                {
                    return _clusterToExportQueue
                        .Values
                        .Any(v => v.AnyExport());
                }
            }

            public IImmutableList<ClusterQueue> GetClusterQueues()
            {
                lock (_clusterToExportQueue)
                {
                    return _clusterToExportQueue
                        .Values
                        .ToImmutableArray();
                }
            }

            public ClusterQueue EnsureClusterQueue(RowItem blockItem, CancellationToken ct)
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

                if (_parameterization.StorageUrls.Any())
                {
                    throw new NotImplementedException(
                        "Support for storage accounts not supported yet");
                }
                else
                {
                    var activity = _parameterization.Activities
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
                    var dmCommandClient = _dbClientFactory.GetDmCommandClient(
                        destinationTableId.ClusterUri,
                        destinationTableId.DatabaseName);
                    var tempStorageUris = await dmCommandClient.GetTempStorageUrisAsync(ct);

                    return tempStorageUris;
                }
            }
            #endregion
        }
        #endregion

        private static readonly TimeSpan CACHE_REFRESH_RATE = TimeSpan.FromMinutes(10);

        private readonly ClusterQueueCollection _clusterQueues;
        private volatile int _isExportingTaskRunning = Convert.ToInt32(false);
        private volatile int _isExportedTaskRunning = Convert.ToInt32(false);

        public SourceTableExportingOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
            _clusterQueues = new(Parameterization, DbClientFactory);
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
            var clusterQueue = _clusterQueues.EnsureClusterQueue(blockItem, ct);

            clusterQueue.EnqueueExport(
                blockItem,
                new KustoPriority(
                    tableIdentity.DatabaseName,
                    new KustoDbPriority(
                        blockItem.IterationId,
                        tableIdentity.TableName,
                        blockItem.BlockId)));

            //  We test since it might be coming from a persisted state which is "exporting" already
            if (blockItem.ParseState<SourceBlockState>() == SourceBlockState.Planned)
            {
                var newBlockItem = blockItem.Clone();

                newBlockItem.State = SourceBlockState.Exporting.ToString();

                await RowItemGateway.AppendAsync(newBlockItem, ct);
            }
            EnsureExportingTask(true, ct);
        }

        private void EnsureExportingTask(bool target, CancellationToken ct)
        {
            if (Interlocked.CompareExchange(
                ref _isExportingTaskRunning,
                Convert.ToInt32(target),
                Convert.ToInt32(!target)) == Convert.ToInt32(!target))
            {
                if (target)
                {
                    BackgroundTaskContainer.AddTask(OnExportingAsync(ct));
                }
            }
        }

        private void EnsureExportedTask(bool target, CancellationToken ct)
        {
            if (Interlocked.CompareExchange(
                ref _isExportedTaskRunning,
                Convert.ToInt32(target),
                Convert.ToInt32(!target)) == Convert.ToInt32(!target))
            {
                if (target)
                {
                    BackgroundTaskContainer.AddTask(OnExportedAsync(ct));
                }
            }
        }

        private async Task OnExportingAsync(CancellationToken ct)
        {
            while (_clusterQueues.AnyExport())
            {
                foreach (var clusterQueue in _clusterQueues.GetClusterQueues())
                {
                    var clusterCache = await clusterQueue.GetClusterCacheAsync(ct);

                    while (clusterQueue.GetOperationIDCount() > clusterCache.ExportCapacity
                        && clusterQueue.TryPeekExport(out var blockItem))
                    {
                        var tableIdentity = blockItem.GetSourceTableIdentity();
                        var commandClient = DbClientFactory.GetDbCommandClient(
                            tableIdentity.ClusterUri,
                            tableIdentity.DatabaseName);
                        var operationId = await commandClient.ExportBlockAsync(
                            clusterCache.ExportRootUris,
                            tableIdentity.TableName,
                            blockItem.CursorStart,
                            blockItem.CursorEnd,
                            blockItem.IngestionTimeStart,
                            blockItem.IngestionTimeEnd,
                            ct);
                        var newBlockItem = blockItem.Clone();

                        newBlockItem.State = SourceBlockState.Exporting.ToString();
                        newBlockItem.OperationId = operationId;
                        await RowItemGateway.AppendAsync(newBlockItem, ct);
                        clusterQueue.AddOperationId(operationId, newBlockItem);
                        EnsureExportedTask(true, ct);
                        //  We've treated it, so we dequeue it
                        clusterQueue.Dequeue();
                    }
                }
            }
            EnsureExportingTask(false, ct);
            //  In case of racing condition
            if (_clusterQueues.AnyExport())
            {
                EnsureExportingTask(true, ct);
            }
        }

        private async Task OnExportedAsync(CancellationToken ct)
        {
            await Task.CompletedTask;
        }
    }
}