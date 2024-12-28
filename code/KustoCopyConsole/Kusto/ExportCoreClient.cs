using Azure;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Core.Tokens;

namespace KustoCopyConsole.Kusto
{
    internal class ExportCoreClient
    {
        #region Inner types
        private record DoubleTask(
            TaskCompletionSource ExportCompletedSource,
            Task SlotReleaseTask);
        #endregion

        private static readonly TimeSpan REFRESH_PERIOD = TimeSpan.FromSeconds(5);

        private readonly DbCommandClient _operationCommandClient;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly object _monitoringLock = new object();
        private IDictionary<string, DoubleTask> _taskMap =
            new Dictionary<string, DoubleTask>();
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
            var exportStartSource = new TaskCompletionSource<string>();
            var exportCompletedSource = new TaskCompletionSource();
            var slotReleasedTask = _queue.RequestRunAsync(
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

                    exportStartSource.SetResult(operationId);
                    //  We want to keep the export slot locked until the export is completed
                    await exportCompletedSource.Task;
                });
            var operationId = await exportStartSource.Task;

            lock (_monitoringLock)
            {
                _taskMap.Add(
                    operationId,
                    new DoubleTask(exportCompletedSource, slotReleasedTask));
            }

            return operationId;
        }

        public void RegisterExistingOperation(string operationId, CancellationToken ct)
        {
            var exportCompletedSource = new TaskCompletionSource();
            var slotReleasedTask = _queue.RequestRunAsync(
                KustoPriority.HighestPriority,
                async () =>
                {
                    //  We want to keep the export slot locked until the export is completed
                    await exportCompletedSource.Task;
                });

            lock (_monitoringLock)
            {
                _taskMap.Add(
                    operationId,
                    new DoubleTask(exportCompletedSource, slotReleasedTask));
            }
        }

        public async Task<IImmutableList<ExportDetail>> AwaitExportAsync(
            long iterationId,
            string tableName,
            string operationId,
            CancellationToken ct)
        {
            Task? oldMonitoringTask = null;
            Task? exportCompletedTask = null;

            lock (_monitoringLock)
            {
                if (!_isMonitoring)
                {
                    _isMonitoring = true;
                    oldMonitoringTask = _monitoringTask;
                    _monitoringTask = MonitorAsync(ct);
                }
                if(_taskMap.ContainsKey(operationId))
                {
                    exportCompletedTask = _taskMap[operationId].ExportCompletedSource.Task;
                }
                else
                {
                    throw new CopyException($"Operation ID lost:  '{operationId}'", false);
                }
            }
            if (oldMonitoringTask != null)
            {
                await oldMonitoringTask;
            }

            await exportCompletedTask;

            return await _operationCommandClient.ShowExportDetailsAsync(
                iterationId,
                tableName,
                operationId,
                ct);
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
                    var lostOperationIds = operationIds
                        .Where(id => !operationStatus.Select(o => o.OperationId).Contains(id));

                    foreach (var op in notPendingOperations)
                    {
                        var exportCompletedSource =
                            _taskMap[op.OperationId].ExportCompletedSource;

                        if (op.State == "Throttled")
                        {
                            exportCompletedSource.SetException(
                                new CopyException($"Throttled:  '{op.Status}'", true));
                        }
                        else if (op.State == "Failed"
                            || op.State == "PartiallySucceeded"
                            || op.State == "Abandoned"
                            || op.State == "BadInput"
                            || op.State == "Canceled"
                            || op.State == "Skipped")
                        {
                            exportCompletedSource.SetException(
                                new CopyException($"Failed:  '{op.Status}'", true));
                        }
                        else
                        {
                            exportCompletedSource.SetResult();
                        }
                    }
                    foreach (var id in lostOperationIds)
                    {
                        var exportCompletedSource = _taskMap[id].ExportCompletedSource;

                        exportCompletedSource.SetException(
                            new CopyException($"Operation ID lost:  '{id}'", false));
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
                        foreach (var exportCompletedSource in
                            _taskMap.Values.Select(o=>o.ExportCompletedSource))
                        {
                            exportCompletedSource.SetException(ex);
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