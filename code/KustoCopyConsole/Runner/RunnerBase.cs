using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class RunnerBase
    {
        public RunnerBase(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
        {
            Parameterization = parameterization;
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
        }

        protected MainJobParameterization Parameterization { get; }

        protected RowItemGateway RowItemGateway { get; }

        protected DbClientFactory DbClientFactory { get; }
    }
}