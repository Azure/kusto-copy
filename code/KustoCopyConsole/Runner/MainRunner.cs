using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Diagnostics;

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : RunnerBase, IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts;

        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            Version appVersion,
            MainJobParameterization parameterization,
            string traceApplicationName,
            CancellationTokenSource cts)
        {
            var credentials = parameterization.CreateCredentials();
            var stagingBlobUriProvider = new AzureBlobUriProvider(
                parameterization.StagingStorageDirectories.Select(s => new Uri(s)),
                credentials);

            Console.Write("Authentication test...");

            await stagingBlobUriProvider.TestAuthenticationAsync(cts.Token);

            Console.WriteLine("  Done");
            Console.Write("Initialize tracking...");

            var databaseTask = TrackDatabase.CreateAsync(
                new Uri($"{parameterization.StagingStorageDirectories.First()}/tracking"),
                credentials,
                cts.Token);
            var dbClientFactoryTask = DbClientFactory.CreateAsync(
                parameterization,
                credentials,
                traceApplicationName,
                cts.Token);
            var database = await databaseTask;

            Console.WriteLine("  Done");
            Console.Write("Initialize Kusto connections...");

            var dbClientFactory = await dbClientFactoryTask;

            Console.WriteLine("  Done");

            var parameters = new RunnerParameters(
                parameterization,
                credentials,
                database,
                dbClientFactory,
                stagingBlobUriProvider);

            return new MainRunner(parameters, cts);
        }

        private MainRunner(RunnerParameters parameters, CancellationTokenSource cts)
            : base(parameters, TimeSpan.Zero)
        {
            _cts = cts;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
            await ((IAsyncDisposable)DbClientFactory).DisposeAsync();
        }

        public async Task RunAsync(CancellationToken ct)
        {
            SyncActivities();
            ReactivateActivities();

            var progressRunner = new ProgressRunner(RunnerParameters);
            var iterationManagementRunner = new IterationManagementRunner(RunnerParameters);
            var planningRunner = new PlanningRunner(RunnerParameters);
            var tempTableRunner = new TempTableCreatingRunner(RunnerParameters);
            var exportingRunner = new ExportingRunner(RunnerParameters);
            var awaitExportedRunner = new AwaitExportedRunner(RunnerParameters);
            var queueIngestRunner = new QueueIngestRunner(RunnerParameters);
            var awaitIngestRunner = new AwaitIngestRunner(RunnerParameters);
            var movingExtentRunner = new MovingExtentRunner(RunnerParameters);
            var awaitMovedExtentRunner = new AwaitMovedRunner(RunnerParameters);
            var blockCompletingRunner = new BlockCompletingRunner(RunnerParameters);
            var blockMetricMaintenanceRunner = new BlockMetricMaintenanceRunner(RunnerParameters);
            var runnerTasks = new[]
            {
                Task.Run(() => progressRunner.RunAsync(ct)),
                Task.Run(() => iterationManagementRunner.RunAsync(ct)),
                Task.Run(() => planningRunner.RunAsync(ct)),
                Task.Run(() => tempTableRunner.RunAsync(ct)),
                Task.Run(() => exportingRunner.RunAsync(ct)),
                Task.Run(() => awaitExportedRunner.RunAsync(ct)),
                Task.Run(() => queueIngestRunner.RunAsync(ct)),
                Task.Run(() => awaitIngestRunner.RunAsync(ct)),
                Task.Run(() => movingExtentRunner.RunAsync(ct)),
                Task.Run(() => awaitMovedExtentRunner.RunAsync(ct)),
                Task.Run(() => blockCompletingRunner.RunAsync(ct)),
                Task.Run(() => blockMetricMaintenanceRunner.RunAsync(ct))
            };
            var monitorTask = Task.Run(async () =>
            {   // Monitor for first failure and cancel
                var remainingTasks = runnerTasks.ToList();

                while (remainingTasks.Count > 0)
                {
                    var completed = await Task.WhenAny(remainingTasks);

                    if (completed.IsFaulted || completed.IsCanceled)
                    {
                        Trace.TraceError("");

                        if (completed.Exception != null)
                        {
                            var ex = completed.Exception;

                            Trace.TraceError(
                                $"Permanent error:  {ex.GetType().Name} '{ex.Message}'");
                            if (ex.InnerException != null)
                            {
                                Trace.TraceError($"   Inner:  {ex.InnerException.GetType().Name}" +
                                    $" '{ex.InnerException.Message}'");
                            }
                            Trace.TraceWarning($"Stack trace:  {ex.StackTrace}");
                            Trace.TraceError("");
                        }
                        await _cts.CancelAsync();
                        break; // Stop monitoring once we've triggered cancellation
                    }

                    remainingTasks.Remove(completed);
                }
            });

            // Wait for all runners to complete (will be fast after cancellation)
            await Task.WhenAll(runnerTasks);
            await monitorTask;
        }

        private void SyncActivities()
        {
            using (var tx = Database.CreateTransaction())
            {   //  Synchronize activities between configuration and database
                var dbActivities = Database.Activities.Query(tx)
                    .ToArray();
                //  Activities in the config but not in the database
                var newActivityNames = Parameterization.Activities
                    .Select(a => a.ActivityName)
                    .Except(dbActivities.Select(a => a.ActivityName));
                //  Disappeared activites
                var oldActivityNames = dbActivities
                    .Select(a => a.ActivityName)
                    .Except(Parameterization.Activities.Select(a => a.ActivityName))
                    .ToHashSet();

                foreach (var a in dbActivities)
                {
                    if (!oldActivityNames.Contains(a.ActivityName))
                    {
                        var paramActivity = Parameterization.GetActivity(a.ActivityName);

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
                    var paramActivity = Parameterization.GetActivity(name);
                    var activity = new ActivityRecord(
                        ActivityState.Active,
                        paramActivity.ActivityName,
                        paramActivity.GetSourceTableIdentity(),
                        paramActivity.GetDestinationTableIdentity());

                    Database.Activities.AppendRecord(activity, tx);
                    Console.WriteLine($"New activity:  '{name}'");
                }

                tx.Complete();
            }
        }

        private void ReactivateActivities()
        {
            if (Parameterization.CopyMode != CopyMode.BackfillOnly)
            {
                using (var tx = Database.CreateTransaction())
                {
                    var hasAnyIncompleteActivities = Database.Activities.Query(tx)
                        .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                        .Any();

                    if (!hasAnyIncompleteActivities)
                    {
                        var activityNames = Parameterization.Activities
                            .Select(a => a.ActivityName);
                        var currentActivitiesQuery = Database.Activities.Query(tx)
                            .Where(pf => pf.Equal(a => a.State, ActivityState.Completed))
                            .Where(pf => pf.In(a => a.ActivityName, activityNames));
                        var currentActivities = currentActivitiesQuery.ToArray();
                        var newActivities = currentActivities
                            .Select(a => a with { State = ActivityState.Active });

                        currentActivitiesQuery.Delete();
                        Database.Activities.AppendRecords(newActivities, tx);
                    }

                    tx.Complete();
                }
            }
        }
    }
}