using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class ExportCoreClient
    {
        private readonly OperationAwaiter _operationAwaiter;
        private readonly DbCommandClient _operationCommandClient;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly IDictionary<string, Task> _slotReleaseTaskMap =
            new Dictionary<string, Task>();

        public ExportCoreClient(DbCommandClient operationCommandClient, int exportCapacity)
        {
            _operationAwaiter = new OperationAwaiter(operationCommandClient);
            _operationCommandClient = operationCommandClient;
            _queue = new(exportCapacity);
        }

        public async Task<string> NewExportAsync(
            IStagingBlobUriProvider blobPathProvider,
            DbCommandClient exportCommandClient,
            string tableName,
            long iterationId,
            string folderName,
            long blockId,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            //  Used to pass the operation ID through
            var exportStartSource = new TaskCompletionSource<string>();
            var slotReleasedTask = _queue.RequestRunAsync(
                new KustoPriority(
                    exportCommandClient.DatabaseName,
                    new KustoDbPriority(iterationId, tableName, blockId)),
                async () =>
                {
                    var rootUris =
                    await blobPathProvider.GetWritableFolderUrisAsync(folderName, ct);
                    var operationId = await exportCommandClient.ExportBlockAsync(
                        rootUris,
                        tableName,
                        cursorStart,
                        cursorEnd,
                        ingestionTimeStart,
                        ingestionTimeEnd,
                        ct);

                    exportStartSource.SetResult(operationId);
                    //  We want to keep the export slot locked until the export is completed
                    await _operationAwaiter.AwaitOperationAsync(operationId, ct);
                });
            var operationId = await exportStartSource.Task;

            lock (_slotReleaseTaskMap)
            {
                _slotReleaseTaskMap.Add(operationId, slotReleasedTask);
            }

            return operationId;
        }

        public async Task<IImmutableList<ExportDetail>> AwaitExportAsync(
            long iterationId,
            string tableName,
            string operationId,
            CancellationToken ct)
        {
            var slotReleaseTask = EnsureExistingOperation(operationId);

            await _operationAwaiter.AwaitOperationAsync(operationId, ct);
            await slotReleaseTask;
            lock (_slotReleaseTaskMap)
            {
                _slotReleaseTaskMap.Remove(operationId);
            }
            
            return await _operationCommandClient.ShowExportDetailsAsync(
                iterationId,
                tableName,
                operationId,
                ct);
        }

        private Task EnsureExistingOperation(string operationId)
        {   //  Catch "orphan" operation IDs that were created in another process
            lock (_slotReleaseTaskMap)
            {
                if (!_slotReleaseTaskMap.Keys.Contains(operationId))
                {
                    var exportCompletedSource = new TaskCompletionSource();
                    var slotReleasedTask = _queue.RequestRunAsync(
                        KustoPriority.HighestPriority,
                        async () =>
                        {
                            //  We want to keep the export slot locked until the export is completed
                            await exportCompletedSource.Task;
                        });

                    _slotReleaseTaskMap.Add(operationId, slotReleasedTask);

                    return slotReleasedTask;
                }
                else
                {
                    return _slotReleaseTaskMap[operationId];
                }
            }
        }
    }
}