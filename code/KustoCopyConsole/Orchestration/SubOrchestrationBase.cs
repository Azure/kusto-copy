using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    internal abstract class SubOrchestrationBase
    {
        protected SubOrchestrationBase(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
        {
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
            Parameterization = parameterization;
        }

        protected RowItemGateway RowItemGateway { get; }
        
        protected DbClientFactory DbClientFactory { get; }

        protected MainJobParameterization Parameterization { get; }

        public abstract Task ProcessAsync(CancellationToken ct);
    }
}