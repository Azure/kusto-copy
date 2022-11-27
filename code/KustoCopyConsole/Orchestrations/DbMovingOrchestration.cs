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
        private readonly ConcurrentDictionary<SubIterationKey, StatusItem> _processingSubIterationMap =
            new ConcurrentDictionary<SubIterationKey, StatusItem>();

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
            var stagedSubIterations = DbStatus.GetIterations()
                .Where(i => i.State <= StatusItemState.Moved)
                .SelectMany(i => DbStatus.GetSubIterations(i.IterationId))
                .Where(s => s.State == StatusItemState.Staged)
                .Where(s => !_processingSubIterationMap.ContainsKey(
                    SubIterationKey.FromSubIteration(s)))
                .OrderBy(s => s.IterationId)
                .ThenBy(s => s.SubIterationId)
                .ToImmutableArray();

            QueueSubIterationsForMoving(stagedSubIterations, ct);
        }

        private void QueueSubIterationsForMoving(
            IEnumerable<StatusItem> subIterations,
            CancellationToken ct)
        {
            foreach (var subIteration in subIterations)
            {
                var key = SubIterationKey.FromSubIteration(subIteration);

                _processingSubIterationMap[key] = subIteration;
                EnqueueUnobservedTask(MoveSubIterationAsync(subIteration, ct), ct);
            }
        }

        private Task MoveSubIterationAsync(StatusItem subIteration, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}