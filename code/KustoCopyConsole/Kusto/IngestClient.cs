using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Ingest.V2;
using KustoCopyConsole.Concurrency;
using System.Collections.Immutable;

namespace KustoCopyConsole.Kusto
{
    internal class IngestClient : KustoClientBase
    {
        private static readonly IImmutableList<IngestStatus> FAILED_STATUS = [
            IngestStatus.Cancelled,
            IngestStatus.Failed,
            IngestStatus.PartialSuccess];

        private readonly IMultiIngest _ingestProvider;
        private readonly string _database;

        public IngestClient(
            IMultiIngest ingestProvider,
            PriorityExecutionQueue<KustoPriority> queue,
            string database)
            : base(queue)
        {
            _ingestProvider = ingestProvider;
            _database = database;
        }

        /// <summary>Queue blobs for ingestion.</summary>
        /// <param name="priority"></param>
        /// <param name="tableName"></param>
        /// <param name="blobUris"></param>
        /// <param name="extentTag"></param>
        /// <param name="creationTime"></param>
        /// <param name="ct"></param>
        /// <returns>List of operation strings.</returns>
        public async Task<IEnumerable<string>> QueueBlobsAsync(
            KustoPriority priority,
            string tableName,
            IEnumerable<Uri> blobUris,
            string extentTag,
            DateTime? creationTime,
            CancellationToken ct)
        {
            const int MAX_BLOBS_PER_BATCH = 20;

            var uriBatches = blobUris.SplitInBatches(MAX_BLOBS_PER_BATCH);
            var properties = new IngestProperties
            {
                CreationTime = creationTime,
                AdditionalTags = new List<string>([extentTag]),
                EnableTracking = true
            };
            var queuingTasks = uriBatches
                .Select(batch => RequestRunAsync(
                    priority,
                    async () =>
                    {
                        var blobSources = batch
                            .Select(u => new BlobSource(u.ToString(), DataSourceFormat.parquet));
                        var operation = await _ingestProvider.IngestAsync(
                            blobSources,
                            _database,
                            tableName,
                            properties,
                            ct);
                        var operationText = operation.ToJsonString();

                        return operationText;
                    }))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(queuingTasks);

            return queuingTasks
                .Select(t => t.Result)
                .ToImmutableArray();
        }

        public async Task<bool> IsIngestionFailureAsync(
            KustoPriority priority,
            string operationText,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var operation = IngestionOperation.FromJsonString(operationText);
                    var summary = await _ingestProvider.GetOperationSummaryAsync(operation, ct);

                    return FAILED_STATUS.Contains(summary.Status);
                });
        }
    }
}