using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

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
            private readonly PriorityQueue<SourceBlockRowItem, KustoPriority> _exportQueue;
            private readonly IDictionary<string, SourceBlockRowItem> _operationIdToBlockMap;
            private readonly Func<CancellationToken, Task<ClusterCache>> _clusterCacheFetch;
            private Task<ClusterCache>? _cacheTask;

            public ClusterQueue(
                Uri clusterUri,
                PriorityQueue<SourceBlockRowItem, KustoPriority> exportQueue,
                IDictionary<string, SourceBlockRowItem> operationIdToBlockMap,
                Func<CancellationToken, Task<ClusterCache>> clusterCacheFetch)
            {
                ClusterUri = clusterUri;
                _exportQueue = exportQueue;
                _operationIdToBlockMap = operationIdToBlockMap;
                _clusterCacheFetch = clusterCacheFetch;
            }

            public Uri ClusterUri { get; }

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

            public void EnqueueExport(SourceBlockRowItem blockItem, KustoPriority kustoPriority)
            {
                lock (_exportQueue)
                {
                    _exportQueue.Enqueue(blockItem, kustoPriority);
                }
            }

            public bool TryPeekExport([MaybeNullWhen(false)] out SourceBlockRowItem blockItem)
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
            public bool AnyOperation()
            {
                lock (_operationIdToBlockMap)
                {
                    return _operationIdToBlockMap.Count != 0;
                }
            }

            public int GetOperationCount()
            {
                lock (_operationIdToBlockMap)
                {
                    return _operationIdToBlockMap.Count;
                }
            }

            public IImmutableList<string> GetOperationIds()
            {
                lock (_operationIdToBlockMap)
                {
                    return _operationIdToBlockMap
                    .Keys
                    .ToImmutableArray();
                }
            }

            public SourceBlockRowItem RemoveBlockItem(string operationId)
            {
                lock (_operationIdToBlockMap)
                {
                    var blockItem = _operationIdToBlockMap[operationId];

                    _operationIdToBlockMap.Remove(operationId);

                    return blockItem;
                }
            }

            public void AddOperationId(SourceBlockRowItem blockItem)
            {
                lock (_operationIdToBlockMap)
                {
                    _operationIdToBlockMap.Add(blockItem.OperationId, blockItem);
                }
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

            public bool AnyOperation()
            {
                lock (_clusterToExportQueue)
                {
                    return _clusterToExportQueue
                        .Values
                        .Any(v => v.AnyOperation());
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

            public ClusterQueue EnsureClusterQueue(SourceBlockRowItem blockItem, CancellationToken ct)
            {
                var tableIdentity = blockItem.SourceTable;

                lock (_clusterToExportQueue)
                {
                    if (!_clusterToExportQueue.ContainsKey(tableIdentity.ClusterUri))
                    {
                        _clusterToExportQueue[tableIdentity.ClusterUri] = new ClusterQueue(
                            tableIdentity.ClusterUri,
                            new PriorityQueue<SourceBlockRowItem, KustoPriority>(),
                            new Dictionary<string, SourceBlockRowItem>(),
                            ctt => FetchClusterCacheAsync(blockItem, ctt));
                    }

                    return _clusterToExportQueue[tableIdentity.ClusterUri];
                }
            }

            #region Fetch
            private async Task<ClusterCache> FetchClusterCacheAsync(
                SourceBlockRowItem blockItem,
                CancellationToken ct)
            {
                var exportRootUrisTask = FetchExportRootUrisAsync(blockItem, ct);
                var exportCapacity = await FetchExportCapacityAsync(blockItem, ct);
                var exportRootUris = await exportRootUrisTask;

                return new ClusterCache(DateTime.Now, exportRootUris, exportCapacity);
            }

            private async Task<int> FetchExportCapacityAsync(
                SourceBlockRowItem blockItem,
                CancellationToken ct)
            {
                var tableIdentity = blockItem.SourceTable;
                var client = _dbClientFactory.GetDbCommandClient(
                    tableIdentity.ClusterUri,
                    tableIdentity.DatabaseName);
                var exportCapacity = await client.GetExportCapacityAsync();

                return exportCapacity;
            }

            private async Task<IImmutableList<Uri>> FetchExportRootUrisAsync(
                SourceBlockRowItem blockItem,
                CancellationToken ct)
            {
                var sourceTableId = blockItem.SourceTable;

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
        private static readonly TimeSpan OPERATION_CHECK_RATE = TimeSpan.FromSeconds(10);

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
                        if (block.RowItem.State == SourceBlockState.Planned)
                        {
                            BackgroundTaskContainer.AddTask(OnQueueExportingItemAsync(
                                block.RowItem,
                                ct));
                        }
                        else if (block.RowItem.State == SourceBlockState.Exporting)
                        {
                            var tableIdentity = block.RowItem.SourceTable;
                            var clusterQueue = _clusterQueues.EnsureClusterQueue(block.RowItem, ct);

                            clusterQueue.AddOperationId(block.RowItem);
                        }
                    }
                }
            }
            EnsureExportedTask(true, ct);

            await Task.CompletedTask;
        }

        protected override void OnProcessRowItemAppended(RowItemAppend e, CancellationToken ct)
        {
            base.OnProcessRowItemAppended(e, ct);

            if (e.Item is SourceBlockRowItem sb && sb.State == SourceBlockState.Planned)
            {
                BackgroundTaskContainer.AddTask(OnQueueExportingItemAsync(
                    sb,
                    ct));
            }
        }

        private async Task OnQueueExportingItemAsync(SourceBlockRowItem blockItem, CancellationToken ct)
        {
            var tableIdentity = blockItem.SourceTable;
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
            if (blockItem.State == SourceBlockState.Planned)
            {
                var newBlockItem = blockItem.ChangeState(SourceBlockState.Exporting);

                await RowItemGateway.AppendAsync(newBlockItem, ct);
            }
            EnsureExportingTask(true, ct);
        }

        private void EnsureExportingTask(bool isExportingTaskRunning, CancellationToken ct)
        {
            if (Interlocked.CompareExchange(
                ref _isExportingTaskRunning,
                Convert.ToInt32(isExportingTaskRunning),
                Convert.ToInt32(!isExportingTaskRunning))
                == Convert.ToInt32(!isExportingTaskRunning))
            {
                if (isExportingTaskRunning)
                {
                    BackgroundTaskContainer.AddTask(OnExportingAsync(ct));
                }
            }
        }

        private void EnsureExportedTask(bool isExportedTaskRunning, CancellationToken ct)
        {
            if (Interlocked.CompareExchange(
                ref _isExportedTaskRunning,
                Convert.ToInt32(isExportedTaskRunning),
                Convert.ToInt32(!isExportedTaskRunning))
                == Convert.ToInt32(!isExportedTaskRunning))
            {
                if (isExportedTaskRunning)
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

                    while (clusterQueue.GetOperationCount() < clusterCache.ExportCapacity
                        && clusterQueue.TryPeekExport(out var blockItem))
                    {
                        var tableIdentity = blockItem.SourceTable;
                        var commandClient = DbClientFactory.GetDbCommandClient(
                            tableIdentity.ClusterUri,
                            tableIdentity.DatabaseName);
                        var sourceTableRowItem = RowItemGateway.InMemoryCache
                            .SourceTableMap[tableIdentity]
                            .IterationMap[blockItem.IterationId]
                            .RowItem;
                        var operationId = await commandClient.ExportBlockAsync(
                            clusterCache.ExportRootUris,
                            tableIdentity.TableName,
                            sourceTableRowItem.CursorStart,
                            sourceTableRowItem.CursorEnd,
                            blockItem.IngestionTimeStart,
                            blockItem.IngestionTimeEnd,
                            ct);
                        var newBlockItem = blockItem.ChangeState(SourceBlockState.Exporting);

                        newBlockItem = newBlockItem with { OperationId = operationId };
                        await RowItemGateway.AppendAsync(newBlockItem, ct);
                        clusterQueue.AddOperationId(newBlockItem);
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
            while (_clusterQueues.AnyOperation())
            {
                foreach (var clusterQueue in _clusterQueues.GetClusterQueues())
                {
                    var clusterCache = await clusterQueue.GetClusterCacheAsync(ct);
                    var operationIds = clusterQueue.GetOperationIds();

                    if (clusterQueue.AnyOperation())
                    {
                        var commandClient = DbClientFactory.GetDbCommandClient(
                            clusterQueue.ClusterUri,
                            string.Empty);
                        var statusList = await commandClient.ShowOperationsAsync(
                            operationIds,
                            ct);

                        if (statusList.Count < operationIds.Count)
                        {
                            throw new NotImplementedException(
                                "Operation ID disapear, need to respawn export");
                        }
                        foreach (var status in statusList)
                        {
                            switch (status.State)
                            {
                                case "Completed":
                                    var blockItem = clusterQueue.RemoveBlockItem(status.OperationId)
                                        .ChangeState(SourceBlockState.Exported);

                                    await RowItemGateway.AppendAsync(blockItem, ct);

                                    break;
                                case "Failed":
                                    throw new CopyException(
                                        $"Failed export '{status.OperationId}':  '{status.Status}'",
                                        false);
                                default:
                                    throw new NotSupportedException($"Export status '{status.State}'");
                            }
                        }
                    }
                }
                await Task.Delay(OPERATION_CHECK_RATE);
            }
            EnsureExportedTask(false, ct);
            //  In case of racing condition
            if (_clusterQueues.AnyOperation())
            {
                EnsureExportedTask(true, ct);
            }
        }
    }
}