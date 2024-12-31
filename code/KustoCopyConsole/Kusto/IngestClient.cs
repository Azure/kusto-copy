using Kusto.Data.Common;
using Kusto.Ingest;

namespace KustoCopyConsole.Kusto
{
    internal class IngestClient
    {
        private readonly IKustoQueuedIngestClient _ingestProvider;
        private readonly string _database;
        private readonly string _table;

        public IngestClient(
            IKustoQueuedIngestClient ingestProvider,
            string database,
            string table)
        {
            _ingestProvider = ingestProvider;
            _database = database;
            _table = table;
        }

        public async Task QueueBlobAsync(
            Uri blobPath,
            string extentTag,
            CancellationToken ct)
        {
            var tagList = new[] { extentTag };
            var properties = new KustoIngestionProperties(_database, _table)
            {
                Format = DataSourceFormat.parquet,
                IngestByTags = tagList,
                DropByTags = tagList,
                IngestIfNotExists = tagList
            };

            await _ingestProvider.IngestFromStorageAsync(
                blobPath.ToString(),
                properties);
        }
    }
}