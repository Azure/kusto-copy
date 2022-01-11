using KustoCopyBookmarks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    internal class KustoExportQueue
    {
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(1);

        private readonly KustoQueuedClient _kustoClient;
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);
        private readonly double _exportSlotsRatio;
        private readonly Task _refreshTask;

        public KustoExportQueue(KustoQueuedClient kustoClient, double exportSlotsRatio)
        {
            _kustoClient = kustoClient;
            _exportSlotsRatio = exportSlotsRatio;
            _refreshTask = RefreshAsync();
        }

        public bool HasAvailability => _executionQueue.HasAvailability;

        public async Task RequestRunAsync(Func<Task> actionAsync)
        {
            await _executionQueue.RequestRunAsync(actionAsync);
        }

        private async Task RefreshAsync()
        {
            while (true)
            {
                var capacities = await _kustoClient.ExecuteCommandAsync(
                    KustoPriority.ExportPriority,
                    string.Empty,
                    ".show capacity | where Resource=='DataExport' | project Total",
                    r => (long)r["Total"]);
                var capacity = capacities.First();
                var newMax = Math.Max(1, (int)(capacity * _exportSlotsRatio));

                _executionQueue.ParallelRunCount = newMax;
                //  Sleep for a while
                await Task.Delay(CAPACITY_REFRESH_PERIOD);
            }
        }
    }
}