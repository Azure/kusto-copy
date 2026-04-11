using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using Spectre.Console;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class ProgressRunner : RunnerBase
    {
        public ProgressRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(20))
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
                        .OrderBy(i => i.IterationKey.IterationId)
                        .ThenBy(i => i.IterationKey.ActivityName)
                        .ToArray();
                    var progressTable = new Table();

                    progressTable.AddColumn("Activity");
                    progressTable.AddColumn("Iteration State");
                    progressTable.AddColumn("Total");
                    progressTable.AddColumn("%");
                    progressTable.AddColumn("Planned");
                    progressTable.AddColumn("Exported");
                    progressTable.AddColumn("Ingested");
                    progressTable.AddColumn("Moved");
                    foreach (var iteration in activeIterations)
                    {
                        var metrics = Database.QueryAggregatedBlockMetrics(iteration.IterationKey, tx);

                        ReportProgress(iteration, metrics, progressTable);
                    }
                    AnsiConsole.Write(progressTable);
                }
                await SleepAsync(ct);
            }
        }

        private void ReportProgress(
            IterationRecord iteration,
            IImmutableDictionary<BlockMetric, long> metrics,
            Table progressTable)
        {
            var totalBlockCount = metrics
                //  Only take the states part of the metrics
                .Where(p => (int)p.Key < Enum.GetValues<BlockState>().Length)
                .Sum(p => p.Value);
            var movedRowCount = metrics[BlockMetric.MovedRowCount];
            var totalPlannedRowCount = metrics[BlockMetric.TotalPlannedRowCount];
            var completionPercentage = totalPlannedRowCount > 0
                ? 100 * movedRowCount / totalPlannedRowCount
                : 0;
            var planned = metrics[BlockMetric.Planned] + metrics[BlockMetric.Exporting];
            var exported = metrics[BlockMetric.Exported] + metrics[BlockMetric.Queued];
            var ingested = metrics[BlockMetric.Ingested] + metrics[BlockMetric.ExtentMoving];
            var moved = metrics[BlockMetric.ExtentMoved];

            progressTable.AddRow(
                iteration.IterationKey.ActivityName,
                iteration.State.ToString(),
                $"{totalBlockCount:N0}",
                $"% {completionPercentage}",
                $"{planned:N0}",
                $"{exported:N0}",
                $"{ingested:N0}",
                $"{moved:N0}");
        }
    }
}