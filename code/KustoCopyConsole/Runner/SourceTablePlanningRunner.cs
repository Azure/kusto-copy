using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class SourceTablePlanningRunner : RunnerBase
    {
        public SourceTablePlanningRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public Task RunAsync(SourceTableRowItem sourceTableRowItem, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}