using Azure.Storage.Files.DataLake;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class DbExportingOrchestration
    {
        #region Inner Types
        private record TableTimeWindowCounts(
            string TableName,
            IImmutableList<TimeWindowCount> TimeWindowCounts);

        private record SubIterationTimeFilter(DateTime? StartTime, DateTime? EndTime);
        #endregion

        private const long TABLE_SIZE_CAP = 1000000000;

        private readonly bool _isContinuousRun;
        private readonly Task _planningTask;
        private readonly SourceDatabaseParameterization _dbParameterization;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoExportQueue _sourceExportQueue;
        private readonly ConcurrentDictionary<long, StatusItem> _processingRecordMap =
            new ConcurrentDictionary<long, StatusItem>();
        private readonly ConcurrentQueue<Task> _unobservedTasksQueue =
            new ConcurrentQueue<Task>();
        private TaskCompletionSource _awaitingActivitiesSource = new TaskCompletionSource();

        #region Constructor
        public static async Task ExportAsync(
            bool isContinuousRun,
            Task planningTask,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoExportQueue sourceExportQueue,
            CancellationToken ct)
        {
            var orchestration = new DbExportingOrchestration(
                isContinuousRun,
                planningTask,
                dbParameterization,
                dbStatus,
                sourceExportQueue);

            await orchestration.RunAsync(ct);
        }

        private DbExportingOrchestration(
            bool isContinuousRun,
            Task planningTask,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoExportQueue sourceExportQueue)
        {
            _isContinuousRun = isContinuousRun;
            _planningTask = planningTask;
            _dbParameterization = dbParameterization;
            _dbStatus = dbStatus;
            _sourceExportQueue = sourceExportQueue;
            _dbStatus.StatusChanged += (sender, e) =>
            {
                _awaitingActivitiesSource.TrySetResult();
            };
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            while (_isContinuousRun
                || !_planningTask.IsCompleted
                || HasUnexportedIterations())
            {   //  Reset task source
                await ObserveTasksAsync();

                var iterations = _dbStatus.GetIterations()
                    .Where(i => i.State == StatusItemState.Initial
                    || i.State == StatusItemState.Planned)
                    .OrderBy(i => i.IterationId);

                foreach (var iteration in iterations)
                {
                    var subIterations = _dbStatus.GetSubIterations(iteration.IterationId)
                        .Where(i => i.State == StatusItemState.Initial
                        || i.State == StatusItemState.Planned)
                        .OrderBy(i => i.SubIterationId);

                    foreach (var subIteration in subIterations)
                    {
                        var recordBatches = _dbStatus.GetRecordBatches(
                            subIteration.IterationId,
                            subIteration.SubIterationId!.Value);
                        var plannedRecordBatches = recordBatches
                            .Where(i => i.State == StatusItemState.Planned)
                            .OrderBy(i => i.RecordBatchId);

                        foreach (var record in plannedRecordBatches)
                        {
                            QueueRecordBatchForExport(record, ct);
                        }
                    }
                }
                //  Wait for activity to continue
                await _awaitingActivitiesSource.Task;
                _awaitingActivitiesSource = new TaskCompletionSource();
                await MarkIterationsAsExportedAsync(ct);
            }
        }

        private bool HasUnexportedIterations()
        {
            return _dbStatus.GetIterations().Any(i => i.State == StatusItemState.Initial
            || i.State == StatusItemState.Planned);
        }

        private async Task ObserveTasksAsync()
        {
            if (_unobservedTasksQueue.TryPeek(out var task))
            {
                if (task.IsCompleted)
                {
                    await task;
                    _unobservedTasksQueue.TryDequeue(out var _);
                    await ObserveTasksAsync();
                }
            }
        }

        private void QueueRecordBatchForExport(StatusItem record, CancellationToken ct)
        {
            if (!_processingRecordMap.ContainsKey(record.RecordBatchId!.Value))
            {   //  Let's try to find the item again to make sure there isn't a racing condition
                record = _dbStatus.GetRecordBatch(
                    record.IterationId,
                    record.SubIterationId!.Value,
                    record.RecordBatchId!.Value);

                //  Ensure the record is still in planned state
                if (record.State == StatusItemState.Planned)
                {
                    _processingRecordMap[record.RecordBatchId!.Value] = record;
                    _unobservedTasksQueue.Enqueue(ExportRecordAsync(record, ct));
                }
            }
        }

        private async Task ExportRecordAsync(StatusItem record, CancellationToken ct)
        {
            await RecordBatchExportingOrchestration.ExportAsync(
                record,
                _dbStatus,
                _sourceExportQueue,
                _dbStatus
                .IndexFolderClient
                .GetSubDirectoryClient(record.TableName)
                .GetSubDirectoryClient(record.RecordBatchId!.Value.ToString("D20")),
                ct);

            if (!_processingRecordMap.TryRemove(record.RecordBatchId!.Value, out var _))
            {
                throw new NotSupportedException("Processing record should have been in map");
            }
        }

        private async Task MarkIterationsAsExportedAsync(CancellationToken ct)
        {
            var itemsToUpdate = new List<StatusItem>();
            var iterations = _dbStatus.GetIterations()
               .Where(i => i.State == StatusItemState.Initial
               || i.State == StatusItemState.Planned);
            var subIterations = iterations
                .SelectMany(i => _dbStatus.GetSubIterations(i.IterationId))
                .Where(s => s.State == StatusItemState.Planned);

            //  Prep sub iterations where all records have been exported
            foreach (var subIteration in subIterations)
            {
                var allRecords = _dbStatus.GetRecordBatches(
                    subIteration.IterationId,
                    subIteration.SubIterationId!.Value);

                if (!allRecords.Any(r => r.State == StatusItemState.Planned))
                {
                    itemsToUpdate.Add(subIteration.UpdateState(StatusItemState.Exported));
                }
            }

            var plannedIterations = _dbStatus.GetIterations()
                .Where(i => i.State == StatusItemState.Planned);

            //  Prep planned iterations where all sub iterations have been exported
            foreach (var iteration in plannedIterations)
            {
                var nonExportedSubIterations = _dbStatus.GetSubIterations(iteration.IterationId)
                    .Where(s => s.State == StatusItemState.Planned
                    || s.State == StatusItemState.Initial)
                    //  Check we didn't slate them for being exported already
                    .Where(s => !itemsToUpdate.Any(
                        i => i.IterationId == s.IterationId && i.SubIterationId == s.SubIterationId));

                if (!nonExportedSubIterations.Any())
                {
                    itemsToUpdate.Add(iteration.UpdateState(StatusItemState.Exported));
                }
            }

            if (itemsToUpdate.Count > 0)
            {
                await _dbStatus.PersistNewItemsAsync(itemsToUpdate, ct);
            }
        }
    }
}