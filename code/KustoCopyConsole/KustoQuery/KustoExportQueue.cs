using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Storage;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoExportQueue
    {
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);

        public KustoExportQueue(
            KustoQueuedClient kustoClient,
            KustoOperationAwaiter kustoOperationAwaiter,
            int concurrentExportCommandCount)
        {
            Client = kustoClient;
            _executionQueue.ParallelRunCount = concurrentExportCommandCount;
        }

        public KustoQueuedClient Client { get; }

        public bool HasAvailability => _executionQueue.HasAvailability;

        public async Task ExportAsync(
            KustoPriority priority,
            IEnumerable<TimeInterval> ingestionTimes,
            DateTime creationTime,
            long expectedRecordCount)
        {
            await Task.CompletedTask;
        }
    }
}