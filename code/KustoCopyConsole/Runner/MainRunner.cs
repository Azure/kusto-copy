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

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : RunnerBase, IAsyncDisposable
    {
        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            MainJobParameterization parameterization,
            string logFilePath,
            CancellationToken ct)
        {
            var appendStorage = CreateAppendStorage(logFilePath);
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

        private static IAppendStorage CreateAppendStorage(string logFilePath)
        {
            return new LocalAppendStorage(GetLocalLogFilePath(logFilePath));
        }

        private static string GetLocalLogFilePath(string logFilePath)
        {
            const string DEFAULT_FILE_NAME = "kusto-copy.log";

            if (string.IsNullOrWhiteSpace(logFilePath))
            {
                return DEFAULT_FILE_NAME;
            }
            else if (Directory.Exists(logFilePath))
            {
                return Path.Combine(logFilePath, DEFAULT_FILE_NAME);
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

                await Task.WhenAll(
                    iterationRunner.RunAsync(ct),
                    tempTableRunner.RunAsync(ct),
                    exportingRunner.RunAsync(ct),
                    awaitExportedRunner.RunAsync(ct),
                    queueIngestRunner.RunAsync(ct),
                    awaitIngestRunner.RunAsync(ct));
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