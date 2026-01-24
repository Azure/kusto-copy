using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class IterationCompletingRunner : RunnerBase
    {
        public IterationCompletingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                await CompleteIterationsAsync(ct);
                await SleepAsync(ct);
            }
        }

        private async Task CompleteIterationsAsync(CancellationToken ct)
        {
            var candidateIterations = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.State, IterationState.Planned))
                .ToImmutableArray();

            foreach (var iteration in candidateIterations)
            {
                var pendingBlockCount = Database.QueryAggregatedBlockMetrics(
                    iteration.IterationKey)
                    .Where(p => p.Key != BlockMetric.ExtentMoved)
                    .Where(p => p.Key != BlockMetric.ExportedRowCount)
                    .Sum(p => p.Value);

                if (pendingBlockCount == 0)
                {
                    var tempTable = GetTempTable(iteration.IterationKey);
                    var destinationTable = Parameterization
                        .Activities[iteration.IterationKey.ActivityName]
                        .GetDestinationTableIdentity();
                    var dbClient = DbClientFactory.GetDbCommandClient(
                        destinationTable.ClusterUri,
                        destinationTable.DatabaseName);

                    await dbClient.DropTableIfExistsAsync(
                        new KustoPriority(iteration.IterationKey),
                        tempTable.TempTableName,
                        ct);
                    CommitCompleteIteration(iteration);
                }
            }
        }

        private void CommitCompleteIteration(IterationRecord iteration)
        {
            Database.TempTables.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                .Delete();
            Database.Iterations.UpdateRecord(
                iteration,
                iteration with
                {
                    State = IterationState.Completed
                });
        }
    }
}