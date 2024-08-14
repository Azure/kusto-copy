using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestration
{
    internal class SourceTablePlanningOrchestration : SubOrchestrationBase
    {
        public SourceTablePlanningOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override Task OnProcessAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}