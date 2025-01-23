using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage.LocalDisk;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kusto.Cloud.Platform.Utils;
using KustoCopyConsole.Storage.AzureStorage;
using Azure.Core;

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : RunnerBase, IAsyncDisposable
    {
        const string DEFAULT_LOG_FILE_NAME = "kusto-copy.log";

        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            MainJobParameterization parameterization,
            string logFilePath,
            CancellationToken ct)
        {
            var appendStorage = CreateAppendStorage(
                logFilePath,
                parameterization.StagingStorageContainers.First(),
                parameterization.GetCredentials());
            var rowItemGateway = await RowItemGateway.CreateAsync(appendStorage, ct);
            var dbClientFactory = await DbClientFactory.CreateAsync(
                parameterization,
                parameterization.GetCredentials(),
                ct);

            return new MainRunner(parameterization, rowItemGateway, dbClientFactory);
        }

        private MainRunner(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
            : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.Zero)
        {
        }

        private static IAppendStorage CreateAppendStorage(
            string logFilePath,
            string storageDirectoryUri,
            TokenCredential credential)
        {
            if (string.IsNullOrWhiteSpace(logFilePath))
            {
                return new AzureBlobAppendStorage(
                    new Uri(storageDirectoryUri),
                    DEFAULT_LOG_FILE_NAME,
                    credential);
            }
            else
            {
                return new LocalAppendStorage(GetLocalLogFilePath(logFilePath));
            }
        }

        private static string GetLocalLogFilePath(string logFilePath)
        {
            if (string.IsNullOrWhiteSpace(logFilePath))
            {
                return DEFAULT_LOG_FILE_NAME;
            }
            else if (Directory.Exists(logFilePath))
            {
                return Path.Combine(logFilePath, DEFAULT_LOG_FILE_NAME);
            }
            else
            {
                return logFilePath;
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)RowItemGateway).DisposeAsync();
            ((IDisposable)DbClientFactory).Dispose();
        }

        public async Task RunAsync(CancellationToken ct)
        {
            CleanUrls();
            await using (var progressBar = new ProgressBar(RowItemGateway.InMemoryCache, ct))
            {
                foreach (var a in Parameterization.Activities.Values)
                {
                    EnsureActivity(a);
                    EnsureIteration(a);
                }
                var iterationRunner =
                    new PlanningRunner(Parameterization, RowItemGateway, DbClientFactory);
                var tempTableRunner =
                    new TempTableCreatingRunner(Parameterization, RowItemGateway, DbClientFactory);
                var exportingRunner =
                    new ExportingRunner(Parameterization, RowItemGateway, DbClientFactory);
                var awaitExportedRunner =
                    new AwaitExportedRunner(Parameterization, RowItemGateway, DbClientFactory);
                var queueIngestRunner =
                    new QueueIngestRunner(Parameterization, RowItemGateway, DbClientFactory);
                var awaitIngestRunner =
                    new AwaitIngestRunner(Parameterization, RowItemGateway, DbClientFactory);
                var moveExtentsRunner =
                    new MoveExtentsRunner(Parameterization, RowItemGateway, DbClientFactory);
                var iterationCompletingRunner =
                    new IterationCompletingRunner(Parameterization, RowItemGateway, DbClientFactory);

                await Task.WhenAll(
                    Task.Run(() => iterationRunner.RunAsync(ct)),
                    Task.Run(() => tempTableRunner.RunAsync(ct)),
                    Task.Run(() => exportingRunner.RunAsync(ct)),
                    Task.Run(() => awaitExportedRunner.RunAsync(ct)),
                    Task.Run(() => queueIngestRunner.RunAsync(ct)),
                    Task.Run(() => awaitIngestRunner.RunAsync(ct)),
                    Task.Run(() => moveExtentsRunner.RunAsync(ct)),
                    Task.Run(() => iterationCompletingRunner.RunAsync(ct)));
            }
        }

        private void CleanUrls()
        {
            foreach (var activity in RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed))
            {
                foreach (var iteration in activity
                    .IterationMap
                    .Values
                    .Where(a => a.RowItem.State != IterationState.Completed))
                {
                    foreach (var block in iteration.BlockMap.Values)
                    {
                        //  A block was sent back to export
                        if (block.RowItem.State == BlockState.Planned
                            //  A block was in the process of exporting
                            || block.RowItem.State == BlockState.Exporting)
                        {
                            UpdateUrls(block.UrlMap.Values, u => u.ChangeState(UrlState.Deleted));
                        }
                        if (block.RowItem.State == BlockState.Exported)
                        {
                            UpdateUrls(
                                block.UrlMap.Values.Where(b => b.RowItem.State == UrlState.Queued),
                                u => u.ChangeState(UrlState.Exported));
                        }
                    }
                }
            }
        }

        private void UpdateUrls(
            IEnumerable<UrlCache> values,
            Func<UrlRowItem, UrlRowItem> updateFunction)
        {
            foreach (var item in values.Select(c => c.RowItem))
            {
                RowItemGateway.Append(updateFunction(item));
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
            var activityItem = cache.ActivityMap[activityParam.ActivityName].RowItem;
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

                RowItemGateway.Append(newIterationItem);
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
            }
        }
    }
}