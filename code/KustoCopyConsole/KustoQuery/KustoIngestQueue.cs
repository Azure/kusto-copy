using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Storage;
using Microsoft.Identity.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoIngestQueue
    {
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly KustoOperationAwaiter _awaiter;

        public KustoIngestQueue(
            KustoQueuedClient kustoClient,
            KustoOperationAwaiter kustoOperationAwaiter,
            int concurrentIngestCommandCount)
        {
            Client = kustoClient;
            _queue = new PriorityExecutionQueue<KustoPriority>(concurrentIngestCommandCount);
            _awaiter = kustoOperationAwaiter;
        }

        public KustoQueuedClient Client { get; }

        public async Task IngestAsync(
            KustoPriority priority,
            IEnumerable<Uri> blobPaths,
            DateTime creationTime,
            IEnumerable<string> tags)
        {
            var pathTexts = blobPaths
                .Select(p => $"'{p};impersonate'");
            var sourceLocatorText = string.Join(Environment.NewLine + ", ", pathTexts);
            var creationTimeText = creationTime.ToString("yyyy-MM-dd HH:mm:ss");
            var tagsText = string.Join(", ", tags.Select(t => $"'{t}'"));
            var commandText = $@".ingest async into table ['{priority.TableName}']
  (
    {sourceLocatorText}
  ) 
  with (
    format='csv',
    persistDetails=true,
    creationTime='{creationTimeText}',
    tags=""[{tagsText}]"")
";
            await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    var operationsIds = await Client.ExecuteCommandAsync(
                        KustoPriority.HighestPriority,
                        priority.DatabaseName!,
                        commandText,
                        r => (Guid)r["OperationId"]);

                    await _awaiter.RunAsynchronousOperationAsync(
                        operationsIds.First(),
                        commandText);
                });
        }
    }
}