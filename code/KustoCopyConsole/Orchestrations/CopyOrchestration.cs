using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;

namespace KustoCopyConsole.Orchestrations
{
    internal class CopyOrchestration
    {
        private readonly MainParameterization _parameterization;

        private CopyOrchestration(MainParameterization parameterization)
        {
            _parameterization = parameterization;
        }

        internal static async Task CopyAsync(MainParameterization parameterization)
        {
            var connectionMaker = ConnectionMaker.Create(parameterization);
            var orchestration = new CopyOrchestration(parameterization);

            await orchestration.RunAsync();
        }

        private Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}