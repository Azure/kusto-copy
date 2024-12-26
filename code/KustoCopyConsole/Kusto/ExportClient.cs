using KustoCopyConsole.Concurrency;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto
{
    internal class ExportClient
    {
        private readonly ExportCoreClient _exportCoreClient;
        private readonly DbCommandClient _exportCommandClient;
        private readonly string _tableName;

        public ExportClient(
            ExportCoreClient exportCoreClient,
            DbCommandClient exportCommandClient,
            string tableName)
        {
            _exportCoreClient = exportCoreClient;
            _exportCommandClient = exportCommandClient;
            _tableName = tableName;
        }

        public async Task<string> NewExportAsync(
            Func<CancellationToken, Task<Uri>> blobPathFactory,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _exportCoreClient.NewExportAsync(
                blobPathFactory,
                _exportCommandClient,
                _tableName,
                cursorStart,
                cursorEnd,
                ingestionTimeStart,
                ingestionTimeEnd,
                ct);
        }

        public async Task AwaitExportAsync(string operationId, CancellationToken ct)
        {
            await _exportCoreClient.AwaitExportAsync(operationId, ct);
        }
    }
}