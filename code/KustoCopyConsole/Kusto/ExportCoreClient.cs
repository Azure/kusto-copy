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
        private readonly PriorityExecutionQueue<KustoDbPriority> _queue;

        public ExportCoreClient(
            DbCommandClient operationCommandClient,
            PriorityExecutionQueue<KustoDbPriority> queue)
        {
            _operationCommandClient = operationCommandClient;
            _queue = queue;
        }

        public async Task<string> NewExportAsync(
            Func<CancellationToken, Task<Uri>> blobPathFactory,
            DbCommandClient exportCommandClient,
            string tableName,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

        public async Task AwaitExportAsync(string operationId, CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

    }
}