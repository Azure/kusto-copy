using Azure.Core;
using Azure.Identity;
using KustoCopyBlobs;
using KustoCopyBlobs.Parameters;

namespace kusto_copy
{
    internal class CopyOrchestration : IAsyncDisposable
    {
        private readonly RootFolderGateway _rootFolderGateway;

        private CopyOrchestration(RootFolderGateway rootFolderGateway)
        {
            _rootFolderGateway = rootFolderGateway;
        }

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            MainParameterization parameterization)
        {
            var credential = new InteractiveBrowserCredential();
            var rootFolderGateway = await RootFolderGateway.CreateFolderGatewayAsync(
                credential,
                dataLakeFolderUrl);

            return new CopyOrchestration(rootFolderGateway);
        }

        public async Task RunAsync()
        {
            //var rootBookmark = await _rootFolderGateway.RetrieveAndLockRootBookmark();

            await ValueTask.CompletedTask;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_rootFolderGateway).DisposeAsync();
        }
    }
}