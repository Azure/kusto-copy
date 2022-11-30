using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Net.NetworkInformation;

namespace KustoCopyConsole.Orchestrations
{
    internal partial class DbStagingOrchestration : DependantOrchestrationBase
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

        private readonly KustoIngestQueue _ingestQueue;
        private readonly ConcurrentDictionary<RecordBatchKey, StatusItem> _processingRecordMap =
            new ConcurrentDictionary<RecordBatchKey, StatusItem>();
        private readonly IDictionary<TableKey, TableState> _tableNameToStateMap =
            new Dictionary<TableKey, TableState>();

        #region Constructor
        public static async Task StageAsync(
            bool isContinuousRun,
            Task exportingTask,
            DatabaseStatus dbStatus,
            KustoIngestQueue ingestQueue,
            CancellationToken ct)
        {
            var orchestration = new DbStagingOrchestration(
                isContinuousRun,
                exportingTask,
                dbStatus,
                ingestQueue);

            await orchestration.RunAsync(ct);
        }

        private DbStagingOrchestration(
            bool isContinuousRun,
            Task planningTask,
            DatabaseStatus dbStatus,
            KustoIngestQueue ingestQueue)
            : base(
                  StatusItemState.Exported,
                  StatusItemState.Staged,
                  isContinuousRun,
                  planningTask,
                  dbStatus)
        {
            _ingestQueue = ingestQueue;
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
                .Where(r => !_processingRecordMap.ContainsKey(RecordBatchKey.FromRecordBatch(r)))
                .OrderBy(i => i.IterationId)
                .ThenBy(i => i.SubIterationId)
                .ThenBy(i => i.RecordBatchId)
                .ToImmutableArray();

            QueueRecordBatchesForStaging(exportedRecordBatches, ct);
        }

        private void QueueRecordBatchesForStaging(
            IEnumerable<StatusItem> recordBatches,
            CancellationToken ct)
        {
            foreach (var record in recordBatches)
            {
                var recordBatchKey = RecordBatchKey.FromRecordBatch(record);

                _processingRecordMap[recordBatchKey] = record;
                EnqueueUnobservedTask(StageRecordBatchAsync(record, ct), ct);
            }
        }

        private async Task StageRecordBatchAsync(StatusItem recordBatch, CancellationToken ct)
        {
            var subIteration = DbStatus.GetSubIteration(
                recordBatch.IterationId,
                recordBatch.SubIterationId!.Value);
            var priority = new KustoPriority(
                recordBatch.IterationId,
                recordBatch.SubIterationId,
                DbStatus.DbName,
                recordBatch.GetStagingTableName(subIteration));

            await SetupStagingTableAsync(recordBatch, priority, ct);
            await IngestRecordBatchAsync(recordBatch, priority, ct);
            UnregisterTableState(recordBatch);

            if (!_processingRecordMap.TryRemove(
                RecordBatchKey.FromRecordBatch(recordBatch),
                out var _))
            {
                throw new NotSupportedException("Processing record should have been in map");
            }
        }

        private async Task SetupStagingTableAsync(
            StatusItem recordBatch,
            KustoPriority priority,
            CancellationToken ct)
        {
            var columns = recordBatch.InternalState!
                .RecordBatchState!
                .ExportRecordBatchState!
                .TableColumns!;
            var isNewState = await RegisterTableStateAsync(recordBatch, columns, ct);

            if (isNewState)
            {   //  Assume the table might exist and fix its schema
                var columnListText = string.Join(
                    ", ",
                    columns.Select(i => $"{i.Name}:{i.Type}"));
                var commandText = $@"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
    .create-merge table ['{priority.TableName}']({columnListText})
    .alter table ['{priority.TableName}']({columnListText})
    .alter table ['{priority.TableName}'] policy merge
    ```
    {{
      ""AllowRebuild"": false,
      ""AllowMerge"": false
    }}
    ```
    .alter table ['{priority.TableName}'] policy caching hot = 0d
    .alter table ['{priority.TableName}'] policy retention 
    ```
    {{
      ""SoftDeletePeriod"": ""40000.00:00:00""
    }}
    ```";

                await _ingestQueue.Client.ExecuteCommandAsync(
                    priority,
                    priority.DatabaseName!,
                    commandText,
                    r => r);
            }
        }

        private void UnregisterTableState(StatusItem recordBatch)
        {
            var tableKey = TableKey.FromRecordBatch(recordBatch);

            lock (_tableNameToStateMap)
            {
                var state = _tableNameToStateMap[tableKey];

                if (state.DecreaseCount() == 0)
                {
                    _tableNameToStateMap.Remove(tableKey);
                }
            }
        }

        private async Task<bool> RegisterTableStateAsync(
            StatusItem recordBatch,
            IImmutableList<TableColumn> columns,
            CancellationToken ct)
        {
            var tableKey = TableKey.FromRecordBatch(recordBatch);
            TableState state;

            lock (_tableNameToStateMap)
            {
                if (_tableNameToStateMap.ContainsKey(tableKey))
                {
                    state = _tableNameToStateMap[tableKey];

                    if (state.Columns.SequenceEqual(columns))
                    {
                        state.IncreaseCount();

                        return false;
                    }
                    else
                    {
                        //  To be continue outside of lock
                    }
                }
                else
                {
                    state = new TableState(columns, 1, new TaskCompletionSource());

                    _tableNameToStateMap.Add(tableKey, state);

                    return true;
                }
            }

            await state.CompletedTask;

            //  Retry
            return await RegisterTableStateAsync(recordBatch, columns, ct);
        }

        private async Task IngestRecordBatchAsync(
            StatusItem recordBatch,
            KustoPriority priority,
            CancellationToken ct)
        {
            var recordState = recordBatch
                .InternalState!
                .RecordBatchState!;
            //  Tag to retrieve the extent IDs with
            var tagValue = Guid.NewGuid().ToString();

            await _ingestQueue.IngestAsync(
                priority,
                recordState!.ExportRecordBatchState!.BlobPaths,
                recordState!.PlanRecordBatchState!.CreationTime!.Value,
                new[] { tagValue });

            var newRecordBatch = recordBatch.UpdateState(StatusItemState.Staged);
            var extentIds = await CleanExtentsAsync(priority, tagValue);

            await ValidateRecordCountAsync(
                priority,
                extentIds,
                recordState!.ExportRecordBatchState!.RecordCount);
            newRecordBatch.InternalState.RecordBatchState!.StageRecordBatchState =
                new StageRecordBatchState
                {
                    ExtentIds = extentIds,
                    TagValue = tagValue
                };

            await DbStatus.PersistNewItemsAsync(new[] { newRecordBatch }, ct);
        }

        private async Task<IImmutableList<string>> CleanExtentsAsync(
            KustoPriority priority,
            string tagValue)
        {
            var commandText = $@".drop extent tags from table ['{priority.TableName}']
('{tagValue}')";
            var extentIds = await _ingestQueue.Client.ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                priority.DatabaseName!,
                commandText,
                r => (string)r["ResultExtentId"]);

            return extentIds;
        }

        private async Task ValidateRecordCountAsync(
            KustoPriority priority,
            IEnumerable<string> extentIds,
            long expectedRecordCount)
        {
            var extentIdsText = string.Join(", ", extentIds.Select(e => $"'{e}'"));
            var queryText = $@"['{priority.TableName}']
| where extent_id() in ({extentIdsText})
| summarize Cardinality=count()
";
            var cardinalities = await _ingestQueue.Client.ExecuteQueryAsync(
                KustoPriority.HighestPriority,
                priority.DatabaseName!,
                queryText,
                r => (long)r["Cardinality"]);
            var totalCardinality = cardinalities.Sum(c => c);

            if (totalCardinality != expectedRecordCount)
            {
                throw new InvalidOperationException(
                    $"Expected ingested record count was {expectedRecordCount} but was"
                    + $"{totalCardinality}");
            }
        }
    }
}