using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.InMemory;
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

        public override async Task ProcessAsync(CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;

            foreach (var a in Parameterization.Activities)
            {
                var tableIdentity = new TableIdentity(
                    NormalizedUri.NormalizeUri(a.Source.ClusterUri),
                    a.Source.DatabaseName,
                    a.Source.TableName);
                var cachedIterations = cache.SourceTableMap.ContainsKey(tableIdentity)
                    ? cache.SourceTableMap[tableIdentity].IterationMap.Values
                    : new SourceTableIterationCache[0];
                var completedItems = cachedIterations
                    .Select(c => c.RowItem)
                    .Where(i => i.ParseState<SourceTableState>() == SourceTableState.Completed);
                var activeItems = cachedIterations
                    .Select(c => c.RowItem)
                    .Where(i => i.ParseState<SourceTableState>() != SourceTableState.Completed);

                if (!cachedIterations.Any()
                    || (a.TableOption.ExportMode != ExportMode.BackFillOnly && !activeItems.Any()))
                {
                    var lastIteration = cachedIterations.Any()
                        ? cachedIterations.ArgMax(i => i.RowItem.IterationId).RowItem
                        : new RowItem();
                    var newIterationId = lastIteration.IterationId + 1;
                    var cursorStart = lastIteration.CursorEnd;

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
            var cursorEnd = await queryClient.GetCurrentCursor(ct);
            var currentItem = new RowItem
            {
                RowType = RowType.SourceTable,
                State = SourceTableState.Planning.ToString(),
                SourceClusterUri = tableIdentity.ClusterUri.ToString(),
                SourceDatabaseName = tableIdentity.DatabaseName,
                SourceTableName = tableIdentity.TableName,
                IterationId = newIterationId,
                CursorStart = cursorStart,
                CursorEnd = cursorEnd
            };

            await RowItemGateway.AppendAsync(currentItem, ct);
        }
    }
}