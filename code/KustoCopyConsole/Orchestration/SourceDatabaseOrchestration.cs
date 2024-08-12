using KustoCopyConsole.Entity;
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
    internal class SourceDatabaseOrchestration : SubOrchestrationBase
    {
        public SourceDatabaseOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        public override async Task ProcessAsync(CancellationToken ct)
        {
            var completedItems = RowItemGateway.InMemoryCache.SoureDatabaseMap.Values
                .Select(c => c.RowItem)
                .Where(i => i.ParseState<SourceDatabaseState>() == SourceDatabaseState.Completed)
                .ToImmutableArray();
            var activeItems = RowItemGateway.InMemoryCache.SoureDatabaseMap.Values
                .Select(c => c.RowItem)
                .Where(i => i.ParseState<SourceDatabaseState>() != SourceDatabaseState.Completed)
                .ToImmutableArray();
            var allSourceDatabases = Parameterization.SourceClusters
                .Select(c => c.Databases.Select(db => new
                {
                    ClusterUri = NormalizedUri.NormalizeUri(c.SourceClusterUri),
                    db.DatabaseName,
                    ClusterParameters = c,
                    DatabaseParameters = db
                }))
                .SelectMany(a => a)
                .ToImmutableDictionary(o => (o.ClusterUri, o.DatabaseName));

            await Task.CompletedTask;
            //if (_sourceCluster.ExportMode == ExportMode.BackFillOnly && completedItems.Any())
            //{   //  We're done
            //}
            ////  Allow GC
            ////completedItems = completedItems.Clear();

            //if (!activeItems.Any())
            //{
            //    var cursorStart = completedItems.Any()
            //        ? completedItems.LastOrDefault()!.CursorEnd
            //        : string.Empty;
            //    var cursorEnd = await _queryClient.GetCurrentCursor(ct);
            //    var currentItem = new RowItem
            //    {
            //        RowType = RowType.SourceDatabase,
            //        State = SourceDatabaseState.Discovering.ToString(),
            //        SourceClusterUri = _sourceCluster.SourceClusterUri,
            //        SourceDatabaseName = _sourceDb.DatabaseName,
            //        IterationId = completedItems.Count(),
            //        CursorStart = cursorStart,
            //        CursorEnd = cursorEnd
            //    };

            //    await RowItemGateway.AppendAsync(currentItem, ct);
            //}
            //else
            //{
            //    throw new NotImplementedException();
            //}
        }
    }
}