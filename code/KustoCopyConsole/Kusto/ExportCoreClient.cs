using KustoCopyConsole.Concurrency;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class ExportCoreClient
    {
        private readonly DbCommandClient _operationCommandClient;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;

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
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

    }
}