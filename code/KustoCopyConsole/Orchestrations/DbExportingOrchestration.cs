using Azure.Storage.Files.DataLake;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class DbExportingOrchestration : DependantOrchestrationBase
    {
        private readonly KustoExportQueue _sourceExportQueue;
        private readonly ConcurrentDictionary<long, StatusItem> _processingRecordMap =
            new ConcurrentDictionary<long, StatusItem>();

        #region Constructor
        public static async Task ExportAsync(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoExportQueue sourceExportQueue,
            CancellationToken ct)
        {
            var orchestration = new DbExportingOrchestration(
                isContinuousRun,
                planningTask,
                dbStatus,
                sourceExportQueue);

            await orchestration.RunAsync(ct);
        }

        private DbExportingOrchestration(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoExportQueue sourceExportQueue)
            : base(
                  StatusItemState.Planned,
                  StatusItemState.Exported,
                  isContinuousRun,
                  planningTask,
                  dbStatus)
        {
            _sourceExportQueue = sourceExportQueue;
        }
        #endregion

        protected override void QueueActivities(CancellationToken ct)
        {
            var plannedRecordBatches = DbStatus.GetIterations()
                //  So the records don't change after the filter
                .ToImmutableArray()
                .Where(i => i.State <= StatusItemState.Planned)
                .SelectMany(i => DbStatus.GetSubIterations(i.IterationId))
                .SelectMany(s => DbStatus.GetRecordBatches(
                    s.IterationId,
                    s.SubIterationId!.Value))
                //  So the records don't change after the filter
                .ToImmutableArray()
                .Where(r => r.State == StatusItemState.Planned)
                .Where(r => !_processingRecordMap.ContainsKey(r.RecordBatchId!.Value))
                .OrderBy(i => i.IterationId)
                .ThenBy(i => i.SubIterationId)
                .ThenBy(i => i.RecordBatchId);

            foreach (var record in plannedRecordBatches)
            {
                QueueRecordBatchesForExport(plannedRecordBatches, ct);
            }
        }

        private void QueueRecordBatchesForExport(
            IEnumerable<StatusItem> recordBatches,
            CancellationToken ct)
        {
            foreach (var record in recordBatches)
            {
                _processingRecordMap[record.RecordBatchId!.Value] = record;
                EnqueueUnobservedTask(ExportRecordAsync(record, ct), ct);
            }
        }

        private async Task ExportRecordAsync(StatusItem record, CancellationToken ct)
        {
            await RecordBatchExportingOrchestration.ExportAsync(
                record,
                DbStatus,
                _sourceExportQueue,
                DbStatus
                .IndexFolderClient
                .GetSubDirectoryClient(record.TableName)
                .GetSubDirectoryClient(record.RecordBatchId!.Value.ToString("D20")),
                ct);

            if (!_processingRecordMap.TryRemove(record.RecordBatchId!.Value, out var _))
            {
                throw new NotSupportedException("Processing record should have been in map");
            }
        }
    }
}