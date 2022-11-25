using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Storage;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoIngestQueue
    {
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);
        private readonly KustoOperationAwaiter _awaiter;

        public KustoIngestQueue(
            KustoQueuedClient kustoClient,
            KustoOperationAwaiter kustoOperationAwaiter,
            int concurrentIngestCommandCount)
        {
            Client = kustoClient;
            _awaiter = kustoOperationAwaiter;
            _executionQueue.ParallelRunCount = concurrentIngestCommandCount;
        }

        public KustoQueuedClient Client { get; }

        public bool HasAvailability => _executionQueue.HasAvailability;

        public async Task<IImmutableList<ExportOutput>> IngestAsync(
            KustoPriority priority,
            IEnumerable<Uri> blobPaths,
            DateTime creationTime,
            long expectedRecordCount)
        {
            var pathTexts = blobPaths
                .Select(p => $"'{p};impersonate'");
            var sourceLocatorText = string.Join(Environment.NewLine + ", ", pathTexts);
            var commandText = $@".ingest async into table ['{priority.TableName}']
  (
    {sourceLocatorText}
  ) 
  with (format='csv', persistDetails=true)
";
            var operationsIds = await Client.ExecuteCommandAsync(
                priority,
                priority.DatabaseName!,
                commandText,
                r => (Guid)r["OperationId"]);
            var outputs = await _awaiter.RunAsynchronousOperationAsync(
                operationsIds.First(),
                r => new ExportOutput(
                    new Uri((string)r["Path"]),
                    (long)r["NumRecords"],
                    (long)r["SizeInBytes"]));

            return outputs;
        }
    }
}