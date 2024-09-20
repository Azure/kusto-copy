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
    /// This orchestration is responsible to manage the life cycle of a destination
    /// table, i.e. create a staging table and drop the staging table.
    /// </summary>
    internal class DestinationTableLifeCycleOrchestration : SubOrchestrationBase
    {
        public DestinationTableLifeCycleOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;

            foreach (var sourceTable in cache.SourceTableMap.Values)
            {
                foreach (var iterationTable in sourceTable.IterationMap.Values)
                {
                    if (iterationTable.RowItem.ParseState<SourceTableState>() !=
                        SourceTableState.Completed)
                    {
                        BackgroundTaskContainer.AddTask(
                            EnsureStagingTableAsync(iterationTable.RowItem, ct));
                    }
                }
            }
            await Task.CompletedTask;
        }

        private async Task EnsureStagingTableAsync(
            RowItem sourceTableIterationItem,
            CancellationToken ct)
        {
            var sourceTableIdentity = sourceTableIterationItem.GetSourceTableIdentity();
            var destinationTableIdentities = MapSourceToDestinations(sourceTableIdentity);

            await Task.CompletedTask;
            throw new NotImplementedException();
        }

        private IEnumerable<TableIdentity> MapSourceToDestinations(
            TableIdentity sourceTableIdentity)
        {
            foreach (var a in Parameterization.Activities)
            {
                if (a.Source.GetTableIdentity() == sourceTableIdentity)
                {
                    foreach(var d in a.Destinations)
                    {
                        yield return d.GetTableIdentity();
                    }
                }
            }
        }
    }
}