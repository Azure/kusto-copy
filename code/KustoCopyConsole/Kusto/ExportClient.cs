using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
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
        private readonly string _kqlQuery;

        public ExportClient(
            ExportCoreClient exportCoreClient,
            DbCommandClient exportCommandClient,
            string tableName,
            string kqlQuery)
        {
            _exportCoreClient = exportCoreClient;
            _exportCommandClient = exportCommandClient;
            _tableName = tableName;
            _kqlQuery = kqlQuery;
        }

        public async Task<string> NewExportAsync(
            KustoPriority priority,
            IStagingBlobUriProvider blobPathProvider,
            long iterationId,
            long blockId,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _exportCoreClient.NewExportAsync(
                priority,
                blobPathProvider,
                _exportCommandClient,
                _kqlQuery,
                iterationId,
                $"iterations/{iterationId:D20}/blocks/{blockId:D20}",
                blockId,
                cursorStart,
                cursorEnd,
                ingestionTimeStart,
                ingestionTimeEnd,
                ct);
        }

        public async Task<IImmutableList<ExportDetail>> AwaitExportAsync(
            KustoPriority priority,
            string operationId,
            CancellationToken ct)
        {
            return await _exportCoreClient.AwaitExportAsync(
                priority,
                operationId,
                ct);
        }
    }
}