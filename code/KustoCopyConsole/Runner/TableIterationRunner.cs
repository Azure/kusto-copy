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
    internal class TableIterationRunner : RunnerBase
    {
        private readonly SourcePlanningRunner _sourceTablePlanningRunner;

        public TableIterationRunner(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
            : base(parameterization, rowItemGateway, dbClientFactory)
        {
            _sourceTablePlanningRunner = new(parameterization, rowItemGateway, dbClientFactory);
        }

        /// <summary>
        /// Complete a new or existing iteration on <paramref name="sourceTableIdentity"/>.
        /// </summary>
        /// <param name="sourceTableIdentity"></param>
        /// <param name="ct"></param>
        /// <returns>
        /// <code>true</code> iif there could be more iterations given configuration.
        /// </returns>
        public async Task<bool> RunAsync(TableIdentity sourceTableIdentity, CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;
            var cachedIterations = cache.SourceTableMap.ContainsKey(sourceTableIdentity)
                ? cache.SourceTableMap[sourceTableIdentity].IterationMap.Values
                : Array.Empty<SourceIterationCache>();
            var completedItems = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State == SourceTableState.Exported);
            var activeItems = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State != SourceTableState.Exported);
            var activityParameterization = Parameterization.Activities
                .Where(a => a.Source.GetTableIdentity() == sourceTableIdentity)
                .First();
            var isBackfillOnly =
                activityParameterization.TableOption.ExportMode == ExportMode.BackfillOnly;

            //  Start new iteration if need to
            if (!cachedIterations.Any() || (!isBackfillOnly && !activeItems.Any()))
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
                    newIterationId,
                    cursorStart,
                    ct);

                activeItems = activeItems.Append(newItem);
            }
            if (activeItems.Any())
            {
                var planningTasks = activeItems
                    .Select(i => new
                    {
                        SourceTableRowItem = i,
                        TempTableMap = CreateTempTableMap(i, ct)
                    })
                    .Select(o => _sourceTablePlanningRunner.RunAsync(
                        o.SourceTableRowItem,
                        o.TempTableMap,
                        ct))
                    .ToImmutableArray();

                await Task.WhenAll(planningTasks);
                throw new NotImplementedException();
            }

            return isBackfillOnly;
        }

        private async Task<SourceTableRowItem> StartIterationAsync(
            TableIdentity tableIdentity,
            long newIterationId,
            string cursorStart,
            CancellationToken ct)
        {
            var queryClient = DbClientFactory.GetDbQueryClient(
                tableIdentity.ClusterUri,
                tableIdentity.DatabaseName);
            var cursorEnd = await queryClient.GetCurrentCursorAsync(ct);
            var hasNewData = await queryClient.HasNewDataAsync(
                tableIdentity.TableName,
                newIterationId,
                cursorStart,
                cursorEnd,
                ct);
            var newIterationItem = new SourceTableRowItem
            {
                State = hasNewData ? SourceTableState.Planning : SourceTableState.Completed,
                SourceTable = tableIdentity,
                IterationId = newIterationId,
                CursorStart = cursorStart,
                CursorEnd = cursorEnd
            };

            await RowItemGateway.AppendAsync(newIterationItem, ct);

            return newIterationItem;
        }

        private IImmutableDictionary<TableIdentity, Task> CreateTempTableMap(
            SourceTableRowItem sourceTableRowItem,
            CancellationToken ct)
        {
            var tempTableCreatingRunner = new DestinationTempTableCreatingRunner(
                Parameterization,
                RowItemGateway,
                DbClientFactory);
            var destinationTables = Parameterization.Activities
                .Where(a => a.Source.GetTableIdentity() == sourceTableRowItem.SourceTable)
                .Select(a => a.Destinations)
                .First();
            var map = destinationTables
                .Select(d => d.GetTableIdentity())
                .Select(i => string.IsNullOrWhiteSpace(i.TableName)
                ? new TableIdentity(
                    i.ClusterUri,
                    i.DatabaseName,
                    sourceTableRowItem.SourceTable.TableName)
                : i)
                .Select(d => new
                {
                    DestinationTable = d,
                    Task = tempTableCreatingRunner.RunAsync(sourceTableRowItem, d, ct)
                })
                .ToImmutableArray()
                .ToImmutableDictionary(o => o.DestinationTable, o => o.Task);

            return map;
        }
    }
}
