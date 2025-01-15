using Azure.Core;
using Azure.Identity;
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
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.RowItems;

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
            : base(parameterization, rowItemGateway, dbClientFactory)
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
            var runTasks = Parameterization.Activities
                .Values
                .Select(a => RunActivityAsync(a, ct))
                .ToImmutableArray();

            await Task.WhenAll(runTasks);
        }

        private async Task RunActivityAsync(
            ActivityParameterization activity,
            CancellationToken ct)
        {
            if (activity.TableOption.ExportMode != ExportMode.BackfillOnly)
            {
                throw new NotSupportedException(
                    $"'{activity.TableOption.ExportMode}' isn't supported yet");
            }

            var sourceTableIdentity = activity.Source.GetTableIdentity();
            var destinationTableIdentity = activity.GetEffectiveDestinationTableIdentity();
            var cache = RowItemGateway.InMemoryCache;
            var cachedIterations = cache.SourceTableMap.ContainsKey(sourceTableIdentity)
                ? cache.SourceTableMap[sourceTableIdentity].IterationMap.Values
                : Array.Empty<IterationCache>();
            var completedIterations = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State == TableState.Completed);
            var activeIterations = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State != TableState.Completed);
            var isBackfillOnly =
                activity.TableOption.ExportMode == ExportMode.BackfillOnly;
            var iterationRunner =
                new IterationRunner(Parameterization, RowItemGateway, DbClientFactory);

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
                    State = TableState.Starting,
                    SourceTable = sourceTableIdentity,
                    DestinationTable = destinationTableIdentity,
                    IterationId = newIterationId,
                    CursorStart = cursorStart,
                    CursorEnd = string.Empty
                };

                await RowItemGateway.AppendAsync(newIterationItem, ct);
                await iterationRunner.RunAsync(newIterationItem, ct);
            }
            else
            {
                await Task.WhenAll(activeIterations
                    .Select(i => iterationRunner.RunAsync(i, ct)));
            }
        }
    }
}