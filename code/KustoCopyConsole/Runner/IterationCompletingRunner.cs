using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class IterationCompletingRunner : RunnerBase
    {
        public IterationCompletingRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(5))
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
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, iteration.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationId, iteration.IterationKey.IterationId))
                    .Where(pf => pf.NotEqual(b => b.State, BlockState.ExtentMoved))
                    .Count();

                if (unmovedBlocks == 0)
                {
                    var tempTable = GetTempTable(iteration.IterationKey);
                    var destinationTable = Parameterization
                        .Activities[iteration.IterationKey.ActivityName]
                        .Destination
                        .GetTableIdentity();
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
            using (var tx = Database.Database.CreateTransaction())
            {
                Database.Iterations.Query(tx)
                    .Where(pf => pf.Equal(
                        i => i.IterationKey.ActivityName,
                        iteration.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(
                        i => i.IterationKey.IterationId,
                        iteration.IterationKey.IterationId))
                    .Delete();
                Database.TempTables.Query(tx)
                    .Where(pf => pf.Equal(
                        i => i.IterationKey.ActivityName,
                        iteration.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(
                        i => i.IterationKey.IterationId,
                        iteration.IterationKey.IterationId))
                    .Delete();
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.ActivityName,
                        iteration.IterationKey.ActivityName))
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationId,
                        iteration.IterationKey.IterationId))
                    .Delete();

                Database.Iterations.AppendRecord(
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