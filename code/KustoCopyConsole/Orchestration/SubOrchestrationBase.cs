using KustoCopyConsole.Entity;
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
            DbClientFactory dbClientFactory)
        {
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
        }

        protected RowItemGateway RowItemGateway { get; }
        
        protected DbClientFactory DbClientFactory { get; }

        public abstract Task ProcessAsync(IEnumerable<RowItem> allItems, CancellationToken ct);
    }
}