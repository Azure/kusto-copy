using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    /// <summary>
    /// Responsible to start and complete iteration.
    /// </summary>
    internal class IterationRunner : RunnerBase
    {
        public IterationRunner(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
            : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        /// <summary>
        /// Complete a new or existing iteration on <paramref name="sourceTableIdentity"/> and
        /// <paramref name="destinationTableIdentity"/>.
        /// </summary>
        /// <param name="sourceTableIdentity"></param>
        /// <param name="destinationTableIdentity"></param>
        /// <param name="ct"></param>
        /// <returns>
        /// <code>true</code> iif there could be more iterations given configuration.
        /// </returns>
        public async Task<bool> RunAsync(
            TableIdentity sourceTableIdentity,
            TableIdentity destinationTableIdentity,
            CancellationToken ct)
        {
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
            var activityParameterization = Parameterization.Activities
                .Where(a => a.Source.GetTableIdentity() == sourceTableIdentity)
                .First();
            var isBackfillOnly =
                activityParameterization.TableOption.ExportMode == ExportMode.BackfillOnly;

            //  Start new iteration if need to
            if (!cachedIterations.Any() || (!isBackfillOnly && !activeIterations.Any()))
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
                var newItem = await StartIterationAsync(
                    sourceTableIdentity,
                    destinationTableIdentity,
                    newIterationId,
                    cursorStart,
                    ct);

                activeIterations = activeIterations.Append(newItem);
            }
            if (activeIterations.Any())
            {
                var planningRunner =
                    new PlanningRunner(Parameterization, RowItemGateway, DbClientFactory);
                var planningTasks = activeIterations
                    .Select(i => new
                    {
                        TableRowItem = i,
                        TempTableTask = CreateTempTableAsync(i, ct)
                    })
                    .Select(o => planningRunner.RunAsync(
                        o.TableRowItem,
                        o.TempTableTask,
                        ct))
                    .ToImmutableArray();

                await Task.WhenAll(planningTasks);
                throw new NotImplementedException();
            }

            return isBackfillOnly;
        }

        private async Task<TableRowItem> StartIterationAsync(
            TableIdentity sourceTableIdentity,
            TableIdentity destinationTableIdentity,
            long newIterationId,
            string cursorStart,
            CancellationToken ct)
        {
            var queryClient = DbClientFactory.GetDbQueryClient(
                sourceTableIdentity.ClusterUri,
                sourceTableIdentity.DatabaseName);
            var cursorEnd = await queryClient.GetCurrentCursorAsync(ct);
            var hasNewData = await queryClient.HasNewDataAsync(
                sourceTableIdentity.TableName,
                newIterationId,
                cursorStart,
                cursorEnd,
                ct);
            var newIterationItem = new TableRowItem
            {
                State = hasNewData ? TableState.Planning : TableState.Completed,
                SourceTable = sourceTableIdentity,
                DestinationTable = destinationTableIdentity,
                IterationId = newIterationId,
                CursorStart = cursorStart,
                CursorEnd = cursorEnd
            };

            await RowItemGateway.AppendAsync(newIterationItem, ct);

            return newIterationItem;
        }

        private Task CreateTempTableAsync(
            TableRowItem sourceTableRowItem,
            CancellationToken ct)
        {
            var tempTableCreatingRunner = new TempTableCreatingRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);
            var createTempTableTask = tempTableCreatingRunner.RunAsync(
                sourceTableRowItem,
                ct);

            return createTempTableTask;
        }
    }
}
