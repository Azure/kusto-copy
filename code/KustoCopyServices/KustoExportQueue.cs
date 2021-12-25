using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    internal class KustoExportQueue
    {
        private readonly KustoClient _kustoClient;
        private readonly TempFolderService _tempFolderService;
        private readonly KustoOperationAwaiter _operationAwaiter;

        public KustoExportQueue(
            KustoClient kustoClient,
            TempFolderService tempFolderService,
            int exportSlotsRatio)
        {
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
            _operationAwaiter = new KustoOperationAwaiter(_kustoClient);
        }

        public async Task<ITempFolderLease> ExportDataAsync(
            string databaseName,
            string tableName,
            string? startCursor,
            string endCursor,
            IEnumerable<DateTime> ingestionTimes)
        {
            await ExecuteExportDataAsync(
                databaseName,
                tableName,
                startCursor,
                endCursor,
                ingestionTimes);

            throw new NotImplementedException();
        }

        private async Task ExecuteExportDataAsync(
            string databaseName,
            string tableName,
            string? startCursor,
            string endCursor,
            IEnumerable<DateTime> ingestionTimes)
        {
            var tempFolderLease = _tempFolderService.LeaseTempFolder();

            try
            {
                var indices = Enumerable.Range(0, ingestionTimes.Count());
                var ingestionTimeList = string.Join(", ", indices.Select(i => $"Time{i}"));
                var commandText = @$"
.export async compressed
to csv (h@'{tempFolderLease.Client.Uri}')
with(namePrefix = 'export', includeHeaders = all, encoding = UTF8NoBOM) <|
table(TargetTableName)
| where cursor_before_or_at(EndCursor)
| where cursor_after(StartCursor)
| where ingestion_time() in ({ingestionTimeList})
";
                var kustoClient = _kustoClient
                    .SetParameter("TargetTableName", tableName)
                    .SetParameter("StartCursor", startCursor ?? string.Empty)
                    .SetParameter("EndCursor", endCursor);

                foreach (var p in ingestionTimes.Zip(indices, (t, i) => (t, i)))
                {
                    kustoClient = kustoClient.SetParameter($"Time{p.i}", p.t);
                }

                var operationIds = await kustoClient
                    .ExecuteCommandAsync(
                    databaseName,
                    commandText,
                    r => (Guid)r["OperationId"]);
                var operationId = operationIds.First();

                await _operationAwaiter.WaitForOperationCompletionAsync(operationId);

                throw new NotImplementedException();
            }
            catch
            {
                tempFolderLease.Dispose();
            }
        }
    }
}