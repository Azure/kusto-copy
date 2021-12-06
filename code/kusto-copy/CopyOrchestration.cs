using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Data.Net.Client;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Parameters;

namespace kusto_copy
{
    internal class CopyOrchestration : IAsyncDisposable
    {
        private readonly RootFolderGateway _rootFolderGateway;

        private CopyOrchestration(
            RootFolderGateway rootFolderGateway)
        {
            _rootFolderGateway = rootFolderGateway;
        }

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            MainParameterization parameterization)
        {
            var credential = new InteractiveBrowserCredential();
            var clusterQueryUri =
                ValidateClusterQueryUri(parameterization.Source!.ClusterQueryUri!);
            var builder = new KustoConnectionStringBuilder(clusterQueryUri.ToString())
                .WithAadUserPromptAuthentication();
            var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
            var rootFolderGateway = await RootFolderGateway.CreateGatewayAsync(
                credential,
                dataLakeFolderUrl);

            return new CopyOrchestration(rootFolderGateway);
        }

        public async Task RunAsync()
        {
            await ValueTask.CompletedTask;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_rootFolderGateway).DisposeAsync();
        }

        private static Uri ValidateClusterQueryUri(string clusterQueryUrl)
        {
            Uri? clusterUri;

            if(Uri.TryCreate(clusterQueryUrl, UriKind.Absolute, out clusterUri))
            {
                return clusterUri;
            }
            else
            {
                throw new CopyException($"Invalid cluster query uri:  '{clusterQueryUrl}'");
            }
        }
    }
}