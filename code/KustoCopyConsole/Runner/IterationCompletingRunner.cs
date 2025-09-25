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
            var candidateIterations = RunnerParameters.Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.State, IterationState.Planned))
                .ToImmutableArray();

            foreach (var iteration in candidateIterations)
            {
                var unmovedBlocks = RunnerParameters.Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey))
                    .Where(pf => pf.NotEqual(b => b.State, BlockState.ExtentMoved))
                    .Count();

                if (unmovedBlocks == 0)
                {
                    var tempTable = GetTempTable(iteration.IterationKey);
                    var destinationTable = RunnerParameters.Parameterization
                        .Activities[iteration.IterationKey.ActivityName]
                        .GetDestinationTableIdentity();
                    var dbClient = RunnerParameters.DbClientFactory.GetDbCommandClient(
                        destinationTable.ClusterUri,
                        destinationTable.DatabaseName);

                    await dbClient.DropTableIfExistsAsync(
                        new KustoPriority(iteration.IterationKey),
                        tempTable.TempTableName,
                        ct);
                    await RunnerParameters.StagingBlobUriProvider.DeleteStagingDirectoryAsync(
                        iteration.IterationKey,
                        ct);
                    CommitCompleteIteration(iteration);
                }
            }
        }

        private void CommitCompleteIteration(IterationRecord iteration)
        {
            using (var tx = RunnerParameters.Database.Database.CreateTransaction())
            {
                RunnerParameters.Database.TempTables.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                    .Delete();
                RunnerParameters.Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey))
                    .Delete();

                RunnerParameters.Database.Iterations.UpdateRecord(
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