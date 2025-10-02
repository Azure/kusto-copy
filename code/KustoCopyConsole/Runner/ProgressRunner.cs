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
                using (var tx = Database.Database.CreateTransaction())
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
            var blocksQuery = Database.Blocks.Query(tx)
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey));
            var blockCount = blocksQuery.Count();
            var plannedCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.Planned))
                .Count();
            var exportingCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.Exporting))
                .Count();
            var exportedCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.Exported))
                .Count();
            var queuedCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.Queued))
                .Count();
            var ingestedCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                .Count();
            var movedCount = blocksQuery
                .Where(pf => pf.Equal(b => b.State, BlockState.ExtentMoved))
                .Count();
            var exportedRowCount = blocksQuery
                .Sum(b => b.ExportedRowCount);

            Console.WriteLine(
                $"Progress {iteration.IterationKey} [{iteration.State}]:  " +
                $"Total={blockCount}, Planned={plannedCount}, " +
                $"Exporting={exportingCount}, Exported={exportedCount}, " +
                $"Queued={queuedCount}, Ingested={ingestedCount}, " +
                $"Moved={movedCount} " +
                $"({exportedRowCount:N0} rows)");
        }
    }
}