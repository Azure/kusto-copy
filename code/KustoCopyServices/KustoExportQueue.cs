using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    internal class KustoExportQueue
    {
        public KustoExportQueue(KustoClient kustoClient, int exportSlotsRatio)
        {
        }

        public Task<ITempFolderLease> ExportDataAsync(
            string databaseName,
            string tableName,
            string? startCursor,
            string endCursor,
            IEnumerable<DateTime> ingestionTimes)
        {
            throw new NotImplementedException();
        }
    }
}