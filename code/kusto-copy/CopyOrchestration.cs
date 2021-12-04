using Azure.Core;
using Azure.Identity;
using KustoCopyAdxIntegration;
using KustoCopyLakeIntegration;
using KustoCopyLakeIntegration.Parameters;

namespace kusto_copy
{
    internal class CopyOrchestration : IAsyncDisposable
    {
        private readonly RootFolderGateway _rootFolderGateway;
        private readonly ClusterQueryGateway _sourceGateway;

        private CopyOrchestration(
            RootFolderGateway rootFolderGateway,
            ClusterQueryGateway sourceGateway)
        {
            _rootFolderGateway = rootFolderGateway;
            _sourceGateway = sourceGateway;
        }

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            MainParameterization parameterization)
        {
            var credential = new InteractiveBrowserCredential();
            var rootFolderGateway = await RootFolderGateway.CreateGatewayAsync(
                credential,
                dataLakeFolderUrl);
            var sourceGateway = await ClusterQueryGateway.CreateGatewayAsync(
                parameterization.Source!.ClusterQueryUri!);

            return new CopyOrchestration(rootFolderGateway, sourceGateway);
        }

        public async Task RunAsync()
        {
            //var rootBookmark = await _rootFolderGateway.RetrieveAndLockRootBookmark();
            _sourceGateway.Hi();

            await ValueTask.CompletedTask;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_rootFolderGateway).DisposeAsync();
        }
    }
}