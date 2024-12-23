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

namespace KustoCopyConsole.Orchestration
{
    /// <summary>
    /// This orchestration is responsible to start iteration and complete them.
    /// </summary>
    internal class SourceTableIterationOrchestration : SubOrchestrationBase
    {
        public SourceTableIterationOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;

            foreach (var a in Parameterization.Activities)
            {
                var tableIdentity = a.Source.GetTableIdentity();
                var cachedIterations = cache.SourceTableMap.ContainsKey(tableIdentity)
                    ? cache.SourceTableMap[tableIdentity].IterationMap.Values
                    : Array.Empty<SourceTableIterationCache>();
                var completedItems = cachedIterations
                    .Select(c => c.RowItem)
                    .Where(i => i.State == SourceTableState.Completed);
                var activeItems = cachedIterations
                    .Select(c => c.RowItem)
                    .Where(i => i.State != SourceTableState.Completed);

                if (!cachedIterations.Any()
                    || (a.TableOption.ExportMode != ExportMode.BackFillOnly && !activeItems.Any()))
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

                    await StartIterationAsync(tableIdentity, newIterationId, cursorStart, ct);
                }
            }
        }

        private async Task StartIterationAsync(
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
        }
    }
}