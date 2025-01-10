using KustoCopyConsole.Concurrency;
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
            IBlobPathProvider blobPathProvider,
            long iterationId,
            long blockId,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _exportCoreClient.NewExportAsync(
                blobPathProvider,
                _exportCommandClient,
                _tableName,
                iterationId,
                blockId,
                cursorStart,
                cursorEnd,
                ingestionTimeStart,
                ingestionTimeEnd,
                ct);
        }

        public async Task<IImmutableList<ExportDetail>> AwaitExportAsync(
            long iterationId,
            string tableName,
            string operationId,
            CancellationToken ct)
        {
            return await _exportCoreClient.AwaitExportAsync(
                iterationId,
                tableName,
                operationId,
                ct);
        }
    }
}