using Kusto.Data.Common;
using Kusto.Ingest;
using System.Diagnostics;

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

        public async Task<string> QueueBlobAsync(
            Uri blobPath,
            string extentTag,
            DateTime? creationTime,
            CancellationToken ct)
        {
            var tagList = new[] { extentTag };
            var properties = new KustoQueuedIngestionProperties(_database, _table)
            {
                Format = DataSourceFormat.parquet,
                DropByTags = tagList,
                ReportLevel = IngestionReportLevel.FailuresAndSuccesses,
                ReportMethod = IngestionReportMethod.Table
            };

            if (creationTime != null)
            {
                properties.AdditionalProperties.Add(
                    "creationTime",
                    creationTime.Value.ToString("o"));
            }

            var ingestionResult = await _ingestProvider.IngestFromStorageAsync(
                blobPath.ToString(),
                properties);
            var serializedResult = IngestionResultSerializer.Serialize(ingestionResult);

            return serializedResult;
        }
    }
}