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
                var unmovedBlocks = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey))
                    .Where(pf => pf.NotEqual(b => b.State, BlockState.ExtentMoved))
                    .Count();

                if (unmovedBlocks == 0)
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
                    await StagingBlobUriProvider.DeleteStagingDirectoryAsync(
                        iteration.IterationKey,
                        ct);
                    CommitCompleteIteration(iteration);
                }
            }
        }

        private void CommitCompleteIteration(IterationRecord iteration)
        {
            const int DELETE_COUNT = 200;

            Database.TempTables.Query()
                .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                .Delete();

            //  Delete batches of blocks at the time not to overrun the RAM
            while (true)
            {
                var deletedCount = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iteration.IterationKey))
                    .Take(DELETE_COUNT)
                    .Delete();

                if (deletedCount < DELETE_COUNT)
                {
                    break;
                }
            }

            Database.Iterations.UpdateRecord(
                iteration,
                iteration with
                {
                    State = IterationState.Completed
                });
        }
    }
}