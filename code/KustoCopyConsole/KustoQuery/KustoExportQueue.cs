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
    public class KustoExportQueue
    {
        private readonly ExecutionQueue _executionQueue = new ExecutionQueue(1);
        private readonly KustoOperationAwaiter _awaiter;

        public KustoExportQueue(
            KustoQueuedClient kustoClient,
            KustoOperationAwaiter kustoOperationAwaiter,
            int concurrentExportCommandCount)
        {
            Client = kustoClient;
            _awaiter = kustoOperationAwaiter;
            _executionQueue.ParallelRunCount = concurrentExportCommandCount;
        }

        public KustoQueuedClient Client { get; }

        public bool HasAvailability => _executionQueue.HasAvailability;

        public async Task<IImmutableList<ExportOutput>> ExportAsync(
            KustoPriority priority,
            Uri folderUri,
            IEnumerable<TimeInterval> ingestionTimes,
            DateTime creationTime,
            long expectedRecordCount)
        {
            var commandText = $@".export async 
  compressed
  to csv (
    h@'{folderUri};impersonate'
  ) 
  <|
  ['{priority.TableName}']
";
            var operationsIds = await Client.ExecuteCommandAsync(
                priority,
                priority.DatabaseName!,
                commandText,
                r => (Guid)r["OperationId"]);
            var outputs = await _awaiter.RunAsynchronousOperationAsync(
                operationsIds.First(),
                r=>new ExportOutput(
                    new Uri((string)r["Path"]),
                    (long)r["NumRecords"],
                    (long)r["SizeInBytes"]));

            return outputs;
        }
    }
}