using Azure.Core;
using Azure.Identity;
using KustoCopyBlobs;

namespace kusto_copy
{
    internal class CopyOrchestration
    {
        private readonly RootFolderGateway _rootFolderGateway;

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            Uri sourceClusterUri)
        {
            var credential = new InteractiveBrowserCredential();
            var rootFolderGateway =
                await RootFolderGateway.CreateFolderGatewayAsync(credential, dataLakeFolderUrl);

            return new CopyOrchestration(rootFolderGateway);
        }

        private CopyOrchestration(RootFolderGateway rootFolderGateway)
        {
            _rootFolderGateway = rootFolderGateway;
        }
    }
}