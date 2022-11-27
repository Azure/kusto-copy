using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net.NetworkInformation;

namespace KustoCopyConsole.Orchestrations
{
    internal partial class DbMovingOrchestration : DependantOrchestrationBase
    {
        #region Inner Types
        private class TableState
        {
            private readonly TaskCompletionSource _taskCompletionSource =
                new TaskCompletionSource();

            public TableState(
                IImmutableList<TableColumn> columns,
                int usageCount,
                TaskCompletionSource taskCompletionSource)
            {
                Columns = columns;
                UsageCount = 1;
            }

            public IImmutableList<TableColumn> Columns { get; }

            public int UsageCount { get; private set; }

            public Task CompletedTask => _taskCompletionSource.Task;

            public int IncreaseCount()
            {
                return ++UsageCount;
            }

            public int DecreaseCount()
            {
                if (--UsageCount == 0)
                {
                    _taskCompletionSource.SetResult();
                }

                return UsageCount;
            }
        }
        #endregion

        private readonly KustoQueuedClient _queuedClient;
        private readonly ConcurrentDictionary<SubIterationKey, StatusItem> _processingSubIteratinMap =
            new ConcurrentDictionary<SubIterationKey, StatusItem>();
        private readonly IDictionary<TableKey, TableState> _tableNameToStateMap =
            new Dictionary<TableKey, TableState>();

        #region Constructor
        public static async Task StageAsync(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbMovingOrchestration(
                isContinuousRun,
                planningTask,
                dbStatus,
                queuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbMovingOrchestration(
            bool isContinuousRun,
            Task stagingTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient)
            : base(
                  StatusItemState.Staged,
                  StatusItemState.Moved,
                  isContinuousRun,
                  stagingTask,
                  dbStatus)
        {
            _queuedClient = queuedClient;
        }
        #endregion

        protected override void QueueActivities(CancellationToken ct)
        {
            var exportedRecordBatches = DbStatus.GetIterations()
                .Where(i => i.State <= StatusItemState.Moved)
                .SelectMany(i => DbStatus.GetSubIterations(i.IterationId))
                .Where(s => s.State == StatusItemState.Staged)
                .Where(s => !_processingSubIteratinMap.ContainsKey(
                    SubIterationKey.FromSubIteration(s)))
                .OrderBy(i => i.IterationId)
                .ThenBy(i => i.SubIterationId)
                .ThenBy(i => i.RecordBatchId)
                .ToImmutableArray();

            QueueSubIterationsForMoving(exportedRecordBatches, ct);
        }

        private void QueueSubIterationsForMoving(
            IEnumerable<StatusItem> recordBatches,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}