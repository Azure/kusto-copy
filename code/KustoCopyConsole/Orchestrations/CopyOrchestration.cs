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
            var lakeFolderBuilder =
                new KustoConnectionStringBuilder(parameterization.LakeFolderConnectionString);
            var sourceBuilder = new KustoConnectionStringBuilder(
                parameterization.Source!.ClusterQueryConnectionString!);
            var sourceKustoClient = new KustoClient(sourceBuilder);
            var sourceQueuedClient = new KustoQueuedClient(
                sourceKustoClient,
                parameterization.Source!.ConcurrentQueryCount,
                parameterization.Source!.ConcurrentExportCommandCount);

            var orchestration = new CopyOrchestration(parameterization);

            await orchestration.RunAsync();
        }

        private Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}