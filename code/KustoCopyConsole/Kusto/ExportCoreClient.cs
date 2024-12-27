using KustoCopyConsole.Concurrency;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class ExportCoreClient
    {
        private static readonly TimeSpan REFRESH_PERIOD = TimeSpan.FromSeconds(5);

        private readonly DbCommandClient _operationCommandClient;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly object _monitoringLock = new object();
        private IDictionary<string, TaskCompletionSource> _taskMap =
            new Dictionary<string, TaskCompletionSource>();
        private bool _isMonitoring = false;
        private Task? _monitoringTask;

        public ExportCoreClient(DbCommandClient operationCommandClient, int exportCapacity)
        {
            _operationCommandClient = operationCommandClient;
            _queue = new(exportCapacity);
        }

        public async Task<string> NewExportAsync(
            Func<CancellationToken, Task<Uri>> blobPathFactory,
            DbCommandClient exportCommandClient,
            string tableName,
            long iterationId,
            long blockId,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                new KustoPriority(
                    exportCommandClient.DatabaseName,
                    new KustoDbPriority(iterationId, tableName, blockId)),
                async () =>
                {
                    var tempUri = await blobPathFactory(ct);
                    var operationId = await exportCommandClient.ExportBlockAsync(
                        tempUri,
                        tableName,
                        cursorStart,
                        cursorEnd,
                        ingestionTimeStart,
                        ingestionTimeEnd,
                        ct);

                    return operationId;
                });
        }

        public async Task AwaitExportAsync(string operationId, CancellationToken ct)
        {
            Task operationTask;
            Task? monitoringTask = null;

            lock (_monitoringLock)
            {
                var taskSource = new TaskCompletionSource();

                _taskMap.Add(operationId, taskSource);
                if (!_isMonitoring)
                {
                    _isMonitoring = true;
                    monitoringTask = _monitoringTask;
                    _monitoringTask = MonitorAsync(ct);
                }

                operationTask = taskSource.Task;
            }
            if (monitoringTask != null)
            {
                await monitoringTask;
            }

            await operationTask;
        }

        private async Task MonitorAsync(CancellationToken ct)
        {
            while (true)
            {
                await Task.Delay(REFRESH_PERIOD, ct);

                try
                {
                    var operationIds = GetOperationIds();
                    var operationStatus =
                        await _operationCommandClient.ShowOperationsAsync(operationIds, ct);
                    var notPendingOperations = operationStatus
                        .Where(s => s.State != "InProgress" && s.State != "Scheduled");

                    foreach (var op in notPendingOperations)
                    {
                        if (op.State == "Throttled")
                        {
                            _taskMap[op.OperationId].SetException(
                                new CopyException($"Throttled:  '{op.Status}'", true));
                        }
                        else if (op.State == "Failed"
                            || op.State == "PartiallySucceeded"
                            || op.State == "Abandoned"
                            || op.State == "BadInput"
                            || op.State == "Canceled"
                            || op.State == "Skipped")
                        {
                            _taskMap[op.OperationId].SetException(
                                new CopyException($"Failed:  '{op.Status}'", true));
                        }
                        else
                        {
                            _taskMap[op.OperationId].SetResult();
                        }
                    }
                    lock (_monitoringLock)
                    {
                        foreach (var notPending in notPendingOperations)
                        {
                            _taskMap.Remove(notPending.OperationId);
                        }
                        if (!_taskMap.Any())
                        {
                            _isMonitoring = false;

                            return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    lock (_monitoringLock)
                    {
                        foreach (var taskSource in _taskMap.Values)
                        {
                            taskSource.SetException(ex);
                        }
                        _taskMap.Clear();
                        _isMonitoring = false;

                        return;
                    }
                }
            }
        }

        private IImmutableList<string> GetOperationIds()
        {
            lock (_monitoringLock)
            {
                return _taskMap.Keys.ToImmutableArray();
            }
        }
    }
}