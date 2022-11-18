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
        private readonly KustoQueuedClient _sourceQueuedClient;
        private readonly ConcurrentDictionary<long, StatusItem> _processingRecordMap =
            new ConcurrentDictionary<long, StatusItem>();
        private readonly ConcurrentQueue<Task> _unobservedTasksQueue = new ConcurrentQueue<Task>();
        private TaskCompletionSource _awaitingActivitiesSource = new TaskCompletionSource();

        #region Constructor
        public static async Task ExportAsync(
            bool isContinuousRun,
            Task planningTask,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbExportingOrchestration(
                isContinuousRun,
                planningTask,
                dbParameterization,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbExportingOrchestration(
            bool isContinuousRun,
            Task planningTask,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
            _isContinuousRun = isContinuousRun;
            _planningTask = planningTask;
            _dbParameterization = dbParameterization;
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
            _dbStatus.IterationActivity += (sender, e) =>
            {
                _awaitingActivitiesSource.SetResult();
            };
            _dbStatus.SubIterationActivity += (sender, e) =>
            {
                _awaitingActivitiesSource.SetResult();
            };
            _dbStatus.PlannedRecordActivity += (sender, e) =>
            {
                _awaitingActivitiesSource.SetResult();
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
                _awaitingActivitiesSource = new TaskCompletionSource();

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
                            QueueRecordBatchForExport(record);
                        }
                    }
                }
                //  Wait for activity to continue
                await _awaitingActivitiesSource.Task;
            }
        }

        private bool HasUnexportedIterations()
        {
            return _dbStatus.GetIterations().Any(i => i.State == StatusItemState.Initial
            || i.State == StatusItemState.Planned);
        }

        private async Task ObserveTasksAsync()
        {
            while (true)
            {
                if (_unobservedTasksQueue.TryPeek(out var task))
                {
                    if (task.IsCompleted)
                    {
                        await task;
                        _unobservedTasksQueue.TryDequeue(out var _);
                    }
                    else
                    {
                        return;
                    }
                }
            }
        }

        private void QueueRecordBatchForExport(StatusItem record)
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
                    _unobservedTasksQueue.Enqueue(ExportRecordAsync(record));
                }
            }
        }

        private Task ExportRecordAsync(StatusItem record)
        {
            throw new NotImplementedException();
        }
    }
}