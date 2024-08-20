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
    /// This orchestration is responsible to export data to storage.
    /// It also is the component fetching the DM storage account if no storage is provided.
    /// </summary>
    internal class SourceTableExportingOrchestration : SubOrchestrationBase
    {
        public SourceTableExportingOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var cache = RowItemGateway.InMemoryCache;

            await Task.CompletedTask;
        }
    }
}