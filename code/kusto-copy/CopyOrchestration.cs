namespace kusto_copy
{
    internal class CopyOrchestration
    {
        private readonly string _dataLakeFolderUrl;
        private readonly Uri _sourceClusterUri;

        public static async Task<CopyOrchestration> CreationOrchestrationAsync(
            string dataLakeFolderUrl,
            Uri sourceClusterUri)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }

        private CopyOrchestration(string dataLakeFolderUrl, Uri sourceClusterUri)
        {
            _dataLakeFolderUrl = dataLakeFolderUrl;
            _sourceClusterUri = sourceClusterUri;
        }
    }
}