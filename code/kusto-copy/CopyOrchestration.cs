namespace kusto_copy
{
    internal class CopyOrchestration
    {
        private readonly string _dataLakeFolderUrl;
        private readonly Uri _sourceClusterUri;

        public CopyOrchestration(string dataLakeFolderUrl, Uri sourceClusterUri)
        {
            _dataLakeFolderUrl = dataLakeFolderUrl;
            _sourceClusterUri = sourceClusterUri;
        }
    }
}