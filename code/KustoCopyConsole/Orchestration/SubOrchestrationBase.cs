using KustoCopyConsole.Entity;
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
        protected SubOrchestrationBase(RowItemGateway rowItemGateway)
        {
            RowItemGateway = rowItemGateway;
        }

        protected RowItemGateway RowItemGateway { get; }

        public abstract Task ProcessAsync(IEnumerable<RowItem> allItems, CancellationToken ct);
    }
}