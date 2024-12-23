﻿using KustoCopyConsole.Entity;
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
        private readonly SourceTablePlanningRunner _sourceTablePlanningRunner;

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
                : Array.Empty<SourceTableIterationCache>();
            var completedItems = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State == SourceTableState.Completed);
            var activeItems = cachedIterations
                .Select(c => c.RowItem)
                .Where(i => i.State != SourceTableState.Completed);
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
                    .Select(i => _sourceTablePlanningRunner.RunAsync(i, ct))
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
            var newIterationItem = new SourceTableRowItem
            {
                State = SourceTableState.Planning,
                SourceTable = tableIdentity,
                IterationId = newIterationId,
                CursorStart = cursorStart,
                CursorEnd = cursorEnd
            };

            await RowItemGateway.AppendAsync(newIterationItem, ct);

            return newIterationItem;
        }
    }
}
