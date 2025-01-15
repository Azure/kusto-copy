using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class OperationAwaiter
    {
        private static readonly TimeSpan REFRESH_PERIOD = TimeSpan.FromSeconds(5);

        private readonly DbCommandClient _operationCommandClient;
        private readonly object _monitoringLock = new object();
        private readonly IDictionary<string, TaskCompletionSource> _completionMap =
            new Dictionary<string, TaskCompletionSource>();
        private bool _isMonitoring = false;
        private Task? _monitoringTask;

        public OperationAwaiter(DbCommandClient operationCommandClient)
        {
            _operationCommandClient = operationCommandClient;
        }

        public async Task AwaitOperationAsync(string operationId, CancellationToken ct)
        {
            var operationCompletionSource = EnsureExistingOperation(operationId);
            Task? oldMonitoringTask = null;

            lock (_monitoringLock)
            {
                if (!_isMonitoring)
                {
                    _isMonitoring = true;
                    oldMonitoringTask = _monitoringTask;
                    _monitoringTask = MonitorAsync(ct);
                }
            }
            if (oldMonitoringTask != null)
            {
                await oldMonitoringTask;
            }
            await operationCompletionSource.Task;
        }

        private TaskCompletionSource EnsureExistingOperation(string operationId)
        {
            lock (_monitoringLock)
            {
                if (!_completionMap.Keys.Contains(operationId))
                {
                    var operationCompletedSource = new TaskCompletionSource();
                    
                    _completionMap.Add(operationId, operationCompletedSource);

                    return operationCompletedSource;
                }
                else
                {
                    return _completionMap[operationId];
                }
            }
        }

        private async Task MonitorAsync(CancellationToken ct)
        {
            while (true)
            {
                await Task.Delay(REFRESH_PERIOD, ct);

                try
                {
                    var operationIds = GetOperationIds();
                    var operationStatus = await _operationCommandClient.ShowOperationsAsync(
                        KustoPriority.HighestPriority,
                        operationIds,
                        ct);
                    var notPendingOperations = operationStatus
                        .Where(s => s.State != "InProgress" && s.State != "Scheduled");
                    var lostOperationIds = operationIds
                        .Where(id => !operationStatus.Select(o => o.OperationId).Contains(id));

                    foreach (var op in notPendingOperations)
                    {
                        var operationCompletedSource = RemoveOperation(op.OperationId);

                        if (op.State == "Throttled")
                        {
                            operationCompletedSource.SetException(
                                new CopyException($"Throttled:  '{op.Status}'", true));
                        }
                        else if (op.State == "Failed"
                            || op.State == "PartiallySucceeded"
                            || op.State == "Abandoned"
                            || op.State == "BadInput"
                            || op.State == "Canceled"
                            || op.State == "Skipped")
                        {
                            operationCompletedSource.SetException(
                                new CopyException(
                                    $"Failed ('{op.State}'):  '{op.Status}'", true));
                        }
                        else
                        {
                            operationCompletedSource.SetResult();
                        }
                    }
                    foreach (var id in lostOperationIds)
                    {
                        var operationCompletedSource = RemoveOperation(id);

                        operationCompletedSource.SetException(
                            new CopyException($"Operation ID lost:  '{id}'", false));
                    }
                    lock (_monitoringLock)
                    {
                        if (!_completionMap.Any())
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
                        foreach (var operationCompletedSource in _completionMap.Values)
                        {
                            operationCompletedSource.SetException(ex);
                        }
                        _completionMap.Clear();
                        _isMonitoring = false;

                        return;
                    }
                }
            }
        }

        private TaskCompletionSource RemoveOperation(string operationId)
        {
            lock (_monitoringLock)
            {
                var source = _completionMap[operationId];

                _completionMap.Remove(operationId);

                return source;
            }
        }

        private IImmutableList<string> GetOperationIds()
        {
            lock (_monitoringLock)
            {
                return _completionMap.Keys.ToImmutableArray();
            }
        }
    }
}