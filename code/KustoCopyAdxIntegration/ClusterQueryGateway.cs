using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace KustoCopyAdxIntegration
{
    public class ClusterQueryGateway
    {
        private readonly ICslAdminProvider _commandProvider;

        public static async Task<ClusterQueryGateway> CreateGatewayAsync(string clusterQueryUrl)
        {
            var builder = new KustoConnectionStringBuilder(clusterQueryUrl)
                .WithAadUserPromptAuthentication();
            var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);

            await ValueTask.CompletedTask;

            return new ClusterQueryGateway(commandProvider);
        }

        private ClusterQueryGateway(ICslAdminProvider commandProvider)
        {
            _commandProvider = commandProvider;
        }

        public void Hi()
        {
            throw new NotImplementedException();
        }
    }
}