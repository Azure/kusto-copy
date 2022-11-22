using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    internal class DbStagingOrchestration : DependantOrchestrationBase
    {
        private readonly KustoQueuedClient _queuedClient;
        private readonly ConcurrentDictionary<long, StatusItem> _processingRecordMap =
            new ConcurrentDictionary<long, StatusItem>();

        #region Constructor
        public static async Task StageAsync(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbStagingOrchestration(
                isContinuousRun,
                planningTask,
                dbStatus,
                queuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbStagingOrchestration(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient)
            : base(
                  StatusItemState.Planned,
                  StatusItemState.Exported,
                  isContinuousRun,
                  planningTask,
                  dbStatus)
        {
            _queuedClient = queuedClient;
        }
        #endregion

        protected override void QueueActivities(CancellationToken ct)
        {
            var exportedRecordBatches = DbStatus.GetIterations()
                .Where(i => i.State <= StatusItemState.Exported)
                .SelectMany(i => DbStatus.GetSubIterations(i.IterationId))
                .SelectMany(s => DbStatus.GetRecordBatches(
                    s.IterationId,
                    s.SubIterationId!.Value))
                .Where(r => r.State == StatusItemState.Exported)
                .Where(r => !_processingRecordMap.ContainsKey(r.RecordBatchId!.Value))
                .OrderBy(i => i.IterationId)
                .ThenBy(i => i.SubIterationId)
                .ThenBy(i => i.RecordBatchId);

            QueueRecordBatchesForStaging(exportedRecordBatches, ct);
        }

        private void QueueRecordBatchesForStaging(
            IEnumerable<StatusItem> recordBatches,
            CancellationToken ct)
        {
            foreach (var record in recordBatches)
            {
                _processingRecordMap[record.RecordBatchId!.Value] = record;
                EnqueueUnobservedTask(StageRecordBatchAsync(record, ct), ct);
            }
        }

        private async Task StageRecordBatchAsync(StatusItem recordBatch, CancellationToken ct)
        {
            await IngestRecordBatchAsync(recordBatch, ct);

            if (!_processingRecordMap.TryRemove(recordBatch.RecordBatchId!.Value, out var _))
            {
                throw new NotSupportedException("Processing record should have been in map");
            }
        }

        private Task IngestRecordBatchAsync(StatusItem recordBatch, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}