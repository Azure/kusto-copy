using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : RunnerBase, IAsyncDisposable
    {
        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            Version appVersion,
            MainJobParameterization parameterization,
            string traceApplicationName,
            CancellationToken ct)
        {
            var credentials = parameterization.CreateCredentials();
            var database = await TrackDatabase.CreateAsync();

            Console.Write("Initialize Kusto connections...");

            var dbClientFactory = await DbClientFactory.CreateAsync(
                parameterization,
                credentials,
                traceApplicationName,
                ct);

            Console.WriteLine("  Done");

            var stagingBlobUriProvider = new AzureBlobUriProvider(
                parameterization.StagingStorageDirectories.Select(s => new Uri(s)),
                credentials);

            return new MainRunner(
                parameterization,
                credentials,
                database,
                dbClientFactory,
                stagingBlobUriProvider);
        }

        private MainRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            DbClientFactory dbClientFactory,
            AzureBlobUriProvider stagingBlobUriProvider)
            : base(
                  parameterization,
                  credential,
                  database,
                  dbClientFactory,
                  stagingBlobUriProvider,
                  TimeSpan.Zero)
        {
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
            ((IDisposable)DbClientFactory).Dispose();
        }

        public async Task RunAsync(CancellationToken ct)
        {
            using (var tx = Database.Database.CreateTransaction())
            {
                SyncActivities(tx);
                EnsureIterations(tx);

                tx.Complete();
            }
            var progressRunner = new ProgressRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var planningRunner = new PlanningRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var tempTableRunner = new TempTableCreatingRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var exportingRunner = new ExportingRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var awaitExportedRunner = new AwaitExportedRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var queueIngestRunner = new QueueIngestRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var awaitIngestRunner = new AwaitIngestRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var moveExtentRunner = new MoveExtentRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var iterationCompletingRunner = new IterationCompletingRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);
            var activityCompletingRunner = new ActivityCompletingRunner(
                Parameterization, Credential, Database, DbClientFactory, StagingBlobUriProvider);

            await TaskHelper.WhenAllWithErrors(
                Task.Run(() => progressRunner.RunAsync(ct)),
                Task.Run(() => planningRunner.RunAsync(ct)),
                Task.Run(() => tempTableRunner.RunAsync(ct)),
                Task.Run(() => exportingRunner.RunAsync(ct)),
                Task.Run(() => awaitExportedRunner.RunAsync(ct)),
                Task.Run(() => queueIngestRunner.RunAsync(ct)),
                Task.Run(() => awaitIngestRunner.RunAsync(ct)),
                Task.Run(() => moveExtentRunner.RunAsync(ct)),
                Task.Run(() => iterationCompletingRunner.RunAsync(ct)),
                Task.Run(() => activityCompletingRunner.RunAsync(ct)));
        }

        private void SyncActivities(TransactionContext tx)
        {
            var allActivities = Database.Activities.Query(tx)
                .ToImmutableArray();
            var newActivityNames = Parameterization.Activities.Keys.Except(
                allActivities.Select(a => a.ActivityName));

            foreach (var a in allActivities)
            {
                if (Parameterization.Activities.TryGetValue(
                    a.ActivityName,
                    out var paramActivity))
                {
                    if (paramActivity.GetSourceTableIdentity() != a.SourceTable)
                    {
                        throw new CopyException(
                            $"Activity '{a.ActivityName}' has mistmached source table ; " +
                            $"configuration is {paramActivity.GetSourceTableIdentity()} while" +
                            $"logs is {a.SourceTable}",
                            false);
                    }
                    else if (paramActivity.GetDestinationTableIdentity() != a.DestinationTable)
                    {
                        throw new CopyException(
                            $"Activity '{a.ActivityName}' has mistmached destination table ; " +
                            $"configuration is {paramActivity.GetDestinationTableIdentity()} while" +
                            $"logs is {a.DestinationTable}",
                            false);
                    }
                }
                else
                {
                    throw new CopyException(
                        $"Activity '{a.ActivityName}' is present in logs but not in " +
                        $"configuration",
                        false);
                }
            }
            foreach (var name in newActivityNames)
            {
                var paramActivity = Parameterization.Activities[name];
                var activity = new ActivityRecord(
                    ActivityState.Active,
                    paramActivity.ActivityName,
                    paramActivity.GetSourceTableIdentity(),
                    paramActivity.GetDestinationTableIdentity());

                Database.Activities.AppendRecord(activity, tx);
                Console.WriteLine($"New activity:  '{name}'");
            }
        }

        private void EnsureIterations(TransactionContext tx)
        {
            foreach (var name in Parameterization.Activities.Keys)
            {
                var lastIteration = Database.Iterations.Query(tx)
                    .Where(pf => pf.Equal(t => t.IterationKey.ActivityName, name))
                    .OrderByDesc(t => t.IterationKey.IterationId)
                    .Take(1)
                    .FirstOrDefault();

                if (lastIteration != null)
                {   //  An iteration already exist, nothing to do
                }
                else
                {   //  Create an iteration
                    var newIterationId = lastIteration != null
                        ? lastIteration.IterationKey.IterationId + 1
                        : 1;
                    var cursorStart = lastIteration != null
                        ? lastIteration.CursorEnd
                        : string.Empty;
                    var newIterationRecord = new IterationRecord(
                        IterationState.Starting,
                        new IterationKey(name, newIterationId),
                        cursorStart,
                        string.Empty);

                    Database.Iterations.AppendRecord(newIterationRecord, tx);
                }
            }
        }
    }
}