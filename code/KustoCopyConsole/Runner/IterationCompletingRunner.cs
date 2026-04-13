using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
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
           : base(parameters, TimeSpan.FromSeconds(20))
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
                if (await IsIterationCompleted(iteration.IterationKey))
                {
                    var directoryDeleteTask = StagingBlobUriProvider.DeleteStagingRootDirectoryAsync(
                        iteration.IterationKey,
                        ct);
                    var tempTable = GetTempTable(iteration.IterationKey);
                    var destinationTable = Parameterization
                        .GetActivity(iteration.IterationKey.ActivityName)
                        .GetDestinationTableIdentity();
                    var dbClient = DbClientFactory.GetDbCommandClient(
                        destinationTable.ClusterUri,
                        destinationTable.DatabaseName);

                    await dbClient.DropTableIfExistsAsync(
                        new KustoPriority(iteration.IterationKey),
                        tempTable.TempTableName,
                        ct);
                    await directoryDeleteTask;
                    CommitCompleteIteration(iteration);
                }
            }
        }

        private async Task<bool> IsIterationCompleted(IterationKey iterationKey)
        {
            using (var tx = Database.CreateTransaction())
            {
                var pendingBlockCount = Database.QueryAggregatedBlockMetrics(iterationKey, tx)
                    .Where(p => p.Key < BlockMetric.ExtentMoved)
                    .Sum(p => p.Value);

                if (pendingBlockCount == 0)
                {
                    await tx.CompleteAsync();

                    return true;
                }
                else
                {
                    tx.Complete();
                 
                    return false;
                }
            }
        }

        private void CommitCompleteIteration(IterationRecord iteration)
        {
            using (var tx = Database.CreateTransaction())
            {
                Database.TempTables.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                    .Delete();
                Database.PlanningPartitions.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                    .Delete();
                Database.Iterations.UpdateRecord(
                    iteration,
                    iteration with
                    {
                        State = IterationState.Completed
                    },
                    tx);

                tx.Complete();
            }
        }
    }
}