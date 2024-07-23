using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    internal class SourceDatabaseOrchestration : SubOrchestrationBase
    {
        private readonly SourceClusterParameterization _sourceCluster;
        private readonly SourceDatabaseParameterization _sourceDb;

        public SourceDatabaseOrchestration(
            RowItemGateway rowItemGateway,
            SourceClusterParameterization sourceCluster,
            SourceDatabaseParameterization sourceDb)
            : base(rowItemGateway)
        {
            _sourceCluster = sourceCluster;
            _sourceDb = sourceDb;
        }

        public override async Task ProcessAsync(IEnumerable<RowItem> allItems, CancellationToken ct)
        {
            var completedItems = allItems
                .Where(i => i.RowType == RowType.SourceDatabase)
                .Where(i => i.ParseState<SourceDatabaseState>() == SourceDatabaseState.Completed)
                .ToImmutableArray();
            var activeItems = allItems
                .Where(i => i.RowType == RowType.SourceDatabase)
                .Where(i => i.ParseState<SourceDatabaseState>() != SourceDatabaseState.Completed)
                .ToImmutableArray();

            if (_sourceCluster.ExportMode == ExportMode.BackFillOnly && completedItems.Any())
            {   //  We're done
            }
            //  Allow GC
            //completedItems = completedItems.Clear();

            if (!activeItems.Any())
            {
                var cursorStart = completedItems.Any()
                    ? completedItems.LastOrDefault()!.CursorEnd
                    : string.Empty;
                var cursorEnd = "Need to call Kusto!";
                var currentItem = new RowItem
                {
                    RowType = RowType.SourceDatabase,
                    State = SourceDatabaseState.Discovering.ToString(),
                    SourceClusterUri = _sourceCluster.SourceClusterUri,
                    SourceDatabaseName = _sourceDb.DatabaseName,
                    IterationId = completedItems.Count(),
                    CursorStart = cursorStart,
                    CursorEnd = cursorEnd
                };

                await RowItemGateway.AppendAsync(currentItem, ct);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}