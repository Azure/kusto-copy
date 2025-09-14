using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using KustoCopyConsole.Storage.AzureStorage;
using System.Collections.Immutable;
using TrackDb.Lib.Predicate;

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
            var fileSystem = new AzureBlobFileSystem(
                parameterization.StagingStorageDirectories.First(),
                credentials);
            var database = await TrackDatabase.CreateAsync();

            Console.Write("Initialize storage...");

            var logStorage = await LogStorage.CreateAsync(fileSystem, appVersion, ct);

            Console.WriteLine("  Done");
            Console.Write("Reading checkpoint logs...");

            var rowItemGateway = await RowItemGateway.CreateAsync(logStorage, ct);

            Console.WriteLine("  Done");
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
                rowItemGateway,
                dbClientFactory,
                stagingBlobUriProvider);
        }

        private MainRunner(
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
                  TimeSpan.Zero)
        {
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)RowItemGateway).DisposeAsync();
            ((IDisposable)DbClientFactory).Dispose();
        }

        public async Task RunAsync(CancellationToken ct)
        {
            SyncActivities();
            EnsureIterations();
            await using (var progressBar = new ProgressBar(RowItemGateway, ct))
            {
                var iterationRunner = new PlanningRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var tempTableRunner = new TempTableCreatingRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var exportingRunner = new ExportingRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var awaitExportedRunner = new AwaitExportedRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var queueIngestRunner = new QueueIngestRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var awaitIngestRunner = new AwaitIngestRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);
                var iterationCompletingRunner = new IterationCompletingRunner(
                    Parameterization, Credential, Database, RowItemGateway, DbClientFactory, StagingBlobUriProvider);

                await TaskHelper.WhenAllWithErrors(
                    Task.Run(() => iterationRunner.RunAsync(ct)),
                    Task.Run(() => tempTableRunner.RunAsync(ct)),
                    Task.Run(() => exportingRunner.RunAsync(ct)),
                    Task.Run(() => awaitExportedRunner.RunAsync(ct)),
                    Task.Run(() => queueIngestRunner.RunAsync(ct)),
                    Task.Run(() => awaitIngestRunner.RunAsync(ct)),
                    Task.Run(() => iterationCompletingRunner.RunAsync(ct)));
            }
        }

        private void SyncActivities()
        {
            using (var tx = Database.Database.CreateTransaction())
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
                        if (paramActivity.Source.GetTableIdentity() != a.SourceTable)
                        {
                            throw new CopyException(
                                $"Activity '{a.ActivityName}' has mistmached source table ; " +
                                $"configuration is {paramActivity.Source} while" +
                                $"logs is {a.SourceTable}",
                                false);
                        }
                        else if (paramActivity.Destination.GetTableIdentity() != a.DestinationTable)
                        {
                            throw new CopyException(
                                $"Activity '{a.ActivityName}' has mistmached destination table ; " +
                                $"configuration is {paramActivity.Destination} while" +
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
                        paramActivity.ActivityName,
                        paramActivity.Source.GetTableIdentity(),
                        paramActivity.Destination.GetTableIdentity());

                    Database.Activities.AppendRecord(activity, tx);
                    Console.WriteLine($"New activity:  '{name}'");
                }

                tx.Complete();
            }
        }

        private void EnsureIterations()
        {
            using (var tx = Database.Database.CreateTransaction())
            {
                foreach (var name in Parameterization.Activities.Keys)
                {
                    var lastIteration = Database.Iterations.Query(tx)
                        .Where(Database.Iterations.PredicateFactory.Equal(
                            t => t.IterationKey.ActivityName, name))
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
}