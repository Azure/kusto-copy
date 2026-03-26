using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class ProgressRunner : RunnerBase
    {
        public ProgressRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                using (var tx = Database.CreateTransaction())
                {
                    var activeIterations = Database.Iterations.Query(tx)
                        .Where(pf => pf.NotEqual(i => i.State, IterationState.Completed))
                        .ToImmutableArray();

                    foreach (var iteration in activeIterations)
                    {
                        ReportProgress(iteration, tx);
                    }
                }
                await SleepAsync(ct);
            }
        }

        private void ReportProgress(IterationRecord iteration, TransactionContext tx)
        {
            var metrics = Database.QueryAggregatedBlockMetrics(iteration.IterationKey, tx);
            var blockCount = metrics
                //  Only take the states part of the metrics
                .Where(p => (int)p.Key < Enum.GetValues<BlockState>().Length)
                .Sum(p => p.Value);
            var movedRowCount = metrics[BlockMetric.MovedRowCount];
            var totalPlannedRowCount = metrics[BlockMetric.TotalPlannedRowCount];
            var completionPercentage = totalPlannedRowCount > 0
                ? 100 * movedRowCount / totalPlannedRowCount
                : 0;

            Console.WriteLine(
                $"Progress [{iteration.State}]:  " +
                $"Total={blockCount}, Planned={metrics[BlockMetric.Planned]}, " +
                $"Exporting={metrics[BlockMetric.Exporting]}, Exported={metrics[BlockMetric.Exported]}, " +
                $"Queued={metrics[BlockMetric.Queued]}, " +
                $"Ingested={metrics[BlockMetric.Ingested]}, " +
                $"Moved={metrics[BlockMetric.ExtentMoved]} - %{completionPercentage}");
        }
    }
}