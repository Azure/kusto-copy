using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Orchestrations;
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
    public class KustoExportQueue
    {
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly KustoOperationAwaiter _awaiter;

        public KustoExportQueue(
            KustoQueuedClient kustoClient,
            KustoOperationAwaiter kustoOperationAwaiter,
            int concurrentExportCommandCount)
        {
            Client = kustoClient;
            _queue = new PriorityExecutionQueue<KustoPriority>(concurrentExportCommandCount);
            _awaiter = kustoOperationAwaiter;
        }

        public KustoQueuedClient Client { get; }

        public async Task<IImmutableList<ExportOutput>> ExportAsync(
            KustoPriority priority,
            Uri folderUri,
            CursorWindow cursorWindow,
            IImmutableList<TimeInterval> ingestionTimes,
            long expectedRecordCount)
        {
            var timeFilters = ingestionTimes
                .Select(i => $"(ingestion_time()>={i.StartTime.ToKql()}" +
                $" and ingestion_time()<={i.EndTime.ToKql()})");
            var commandText = $@"
.export async compressed
to csv (
    h@'{folderUri};impersonate'
)
<|
['{priority.TableName}']
{cursorWindow.ToCursorKustoPredicate()}
| where {string.Join(Environment.NewLine + " or ", timeFilters)}
";
            var outputs = await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    var operationsIds = await Client.ExecuteCommandAsync(
                        KustoPriority.HighestPriority,
                        priority.DatabaseName!,
                        commandText,
                        r => (Guid)r["OperationId"]);
                    var outputs = await _awaiter.RunAsynchronousOperationAsync(
                        operationsIds.First(),
                        commandText,
                        r => new ExportOutput(
                            new Uri((string)r["Path"]),
                            (long)r["NumRecords"],
                            (long)r["SizeInBytes"]));
                    var totalRecordCount = outputs.Sum(o => o.RecordCount);

                    if (expectedRecordCount != totalRecordCount)
                    {
                        throw new CopyException(
                            $"Expected to export {expectedRecordCount} records"
                            + $" but exported {totalRecordCount}");
                    }

                    return outputs;
                });

            return outputs;
        }
    }
}