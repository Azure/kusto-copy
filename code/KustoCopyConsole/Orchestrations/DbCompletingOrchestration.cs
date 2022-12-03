﻿using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Net.NetworkInformation;

namespace KustoCopyConsole.Orchestrations
{
    internal partial class DbCompletingOrchestration : DependantOrchestrationBase
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
            Task movingTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbCompletingOrchestration(
                isContinuousRun,
                movingTask,
                dbStatus,
                queuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbCompletingOrchestration(
            bool isContinuousRun,
            Task movingTask,
            DatabaseStatus dbStatus,
            KustoQueuedClient queuedClient)
            : base(
                  StatusItemState.Moved,
                  StatusItemState.Complete,
                  isContinuousRun,
                  movingTask,
                  dbStatus)
        {
            _queuedClient = queuedClient;
        }
        #endregion

        protected override void QueueActivities(CancellationToken ct)
        {
            var movedSubIterations = DbStatus.GetIterations()
                .Where(i => i.State <= StatusItemState.Complete)
                .SelectMany(i => DbStatus.GetSubIterations(i.IterationId))
                .Where(s => s.State == StatusItemState.Moved)
                .Where(s => !_processingSubIterationMap.ContainsKey(
                    SubIterationKey.FromSubIteration(s)))
                .OrderBy(s => s.IterationId)
                .ThenBy(s => s.SubIterationId)
                .ToImmutableArray();

            QueueSubIterationsForCompleting(movedSubIterations, ct);
        }

        private void QueueSubIterationsForCompleting(
            IEnumerable<StatusItem> subIterations,
            CancellationToken ct)
        {
            foreach (var subIteration in subIterations)
            {
                var key = SubIterationKey.FromSubIteration(subIteration);

                _processingSubIterationMap[key] = subIteration;
                EnqueueUnobservedTask(CompleteSubIterationAsync(subIteration, ct), ct);
            }
        }

        private async Task CompleteSubIterationAsync(
            StatusItem subIteration,
            CancellationToken ct)
        {
            var dropTablesTask = DropStagingTablesAsync(subIteration);
            var deleteFolderTask = DeleteSubIterationFolderAsync(subIteration);

            await Task.WhenAll(dropTablesTask, deleteFolderTask);

            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var newSubIteration = subIteration.UpdateState(StatusItemState.Complete);
            var newRecordBatches = DbStatus
                .GetRecordBatches(subIterationKey.IterationId, subIterationKey.SubIterationId)
                .Select(r => r.UpdateState(StatusItemState.Complete));
            var allItems = newRecordBatches.Prepend(newSubIteration).ToImmutableArray();

            await DbStatus.PersistNewItemsAsync(allItems, ct);
        }

        private async Task DropStagingTablesAsync(StatusItem subIteration)
        {
            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var priority = new KustoPriority(
               subIterationKey.IterationId,
               subIterationKey.SubIterationId,
               DbStatus.DbName);
            var tableNames = DbStatus
                .GetRecordBatches(subIterationKey.IterationId, subIterationKey.SubIterationId)
                .Select(r => r.GetStagingTableName(subIteration))
                .Distinct();
            var tableNamesText = string.Join(", ", tableNames.Select(t => $"['{t}']"));
            var commandText = $@".drop tables ({tableNamesText}) ifexists";

            await _queuedClient.ExecuteCommandAsync(
                priority,
                DbStatus.DbName,
                commandText,
                r => r);
        }

        private async Task DeleteSubIterationFolderAsync(StatusItem subIteration)
        {
            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var folderClient = DbStatus.GetSubIterationFolderClient(
                subIterationKey.IterationId,
                subIterationKey.SubIterationId);

            await folderClient.DeleteIfExistsAsync();
        }
    }
}