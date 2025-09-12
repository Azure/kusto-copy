using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using KustoCopyConsole.Storage.AzureStorage;

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
            DisplayExistingIterations();
            await using (var progressBar = new ProgressBar(RowItemGateway, ct))
            {
                foreach (var a in Parameterization.Activities.Values)
                {
                    EnsureActivity(a);
                    EnsureIteration(a);
                }
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

        private static void DisplayIteration(IterationRowItem item, bool isNew)
        {
            var iterationAge = isNew
                ? "New"
                : "Existing";

            Console.WriteLine(
                $"{iterationAge} iteration {item.GetIterationKey()}:  " +
                $"['{item.CursorStart}', '{item.CursorEnd}']");
        }

        private void DisplayExistingIterations()
        {
            var cache = RowItemGateway.InMemoryCache;
            var existingIterations = cache.ActivityMap
                .Values
                .SelectMany(a => a.IterationMap.Values)
                .Select(i => i.RowItem)
                .Where(i => i.State != IterationState.Completed);

            foreach (var iteration in existingIterations)
            {
                DisplayIteration(iteration, false);
            }
        }

        private void EnsureIteration(ActivityParameterization activityParam)
        {
            if (activityParam.TableOption.ExportMode != ExportMode.BackfillOnly)
            {
                throw new NotSupportedException(
                    $"'{activityParam.TableOption.ExportMode}' isn't supported yet");
            }

            var cache = RowItemGateway.InMemoryCache;
            var cachedIterations = cache.ActivityMap.ContainsKey(activityParam.ActivityName)
                ? cache.ActivityMap[activityParam.ActivityName].IterationMap.Values
                : Array.Empty<IterationCache>();
            var completedIterations = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State == IterationState.Completed);
            var activeIterations = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State != IterationState.Completed);
            var isBackfillOnly =
                activityParam.TableOption.ExportMode == ExportMode.BackfillOnly;

            //  Start new iteration if need to
            if (!cachedIterations.Any())
            {
                var lastIteration = cachedIterations.Any()
                    ? cachedIterations.ArgMax(i => i.RowItem.IterationId).RowItem
                    : null;
                var newIterationId = lastIteration != null
                    ? lastIteration.IterationId + 1
                    : 1;
                var cursorStart = lastIteration != null
                    ? lastIteration.CursorEnd
                    : string.Empty;
                var newIterationItem = new IterationRowItem
                {
                    State = IterationState.Starting,
                    ActivityName = activityParam.ActivityName,
                    IterationId = newIterationId,
                    CursorStart = cursorStart,
                    CursorEnd = string.Empty
                };
                var iterationKey = newIterationItem.GetIterationKey();

                RowItemGateway.Append(newIterationItem);
                DisplayIteration(newIterationItem, true);
            }
        }

        private void EnsureActivity(ActivityParameterization activityParam)
        {
            if (!RowItemGateway.InMemoryCache.ActivityMap.ContainsKey(activityParam.ActivityName))
            {
                var activity = new ActivityRowItem
                {
                    State = ActivityState.Active,
                    ActivityName = activityParam.ActivityName,
                    SourceTable = activityParam.Source.GetTableIdentity(),
                    DestinationTable = activityParam.GetEffectiveDestinationTableIdentity()
                };

                RowItemGateway.Append(activity);
                Console.WriteLine($"New activity:  '{activity.ActivityName}'");
            }
        }
    }
}