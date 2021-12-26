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

        public KustoExportQueue(KustoClient kustoClient, int exportSlotsRatio)
        {
            _kustoClient = kustoClient;
        }

        public async Task RequestRunAsync(
            string databaseName,
            string tableName,
            string? startCursor,
            string endCursor,
            DateTime ingestionTimes)
        {
            await ValueTask.CompletedTask;
        }
    }
}