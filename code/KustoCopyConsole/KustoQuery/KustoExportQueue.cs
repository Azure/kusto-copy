using KustoCopyConsole.Concurrency;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoExportQueue
    {
        private readonly KustoQueuedClient _kustoClient;
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);

        public KustoExportQueue(KustoQueuedClient kustoClient, int concurrentExportCommandCount)
        {
            _kustoClient = kustoClient;
            _executionQueue.ParallelRunCount = concurrentExportCommandCount;
        }

        public bool HasAvailability => _executionQueue.HasAvailability;

        public async Task RequestRunAsync(Func<Task> actionAsync)
        {
            await RequestRunAsync(async () =>
            {
                await actionAsync();

                return 0;
            });
        }

        public async Task<T> RequestRunAsync<T>(Func<Task<T>> functionAsync)
        {
            return await _executionQueue.RequestRunAsync(functionAsync);
        }
    }
}