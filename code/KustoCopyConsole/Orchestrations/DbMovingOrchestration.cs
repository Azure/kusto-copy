using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
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

        private async Task MoveSubIterationAsync(StatusItem subIteration, CancellationToken ct)
        {
            await EnsureAllTablesCreatedAsync(subIteration);
            await MoveAllExtentsAsync(subIteration);

            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var newSubIteration = subIteration.UpdateState(StatusItemState.Moved);
            var newRecordBatches = DbStatus
                .GetRecordBatches(subIterationKey.IterationId, subIterationKey.SubIterationId)
                .Select(r => r.UpdateState(StatusItemState.Moved));
            var allItems = newRecordBatches.Prepend(newSubIteration).ToImmutableArray();

            await DbStatus.PersistNewItemsAsync(allItems, ct);
        }

        #region Table Schema
        private async Task EnsureAllTablesCreatedAsync(StatusItem subIteration)
        {
            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var priority = new KustoPriority(
                subIterationKey.IterationId,
                subIterationKey.SubIterationId,
                DbStatus.DbName);
            var tables = DbStatus
                .GetRecordBatches(subIterationKey.IterationId, subIterationKey.SubIterationId)
                .GroupBy(r => r.TableName)
                .Select(g => g.MaxBy(r => r.Timestamp)!)
                .Select(r => new
                {
                    TableName = r.TableName,
                    Schema = r.InternalState!.RecordBatchState!.ExportRecordBatchState!.TableColumns!
                })
                .ToImmutableArray();
            var existingTables = await ExistingTablesAsync(
                tables.Select(t => t.TableName),
                priority);
            var commands = tables
                .SelectMany(t => GetTableSchemaCommands(
                    t.TableName,
                    existingTables.Contains(t.TableName),
                    t.Schema));
            var commandText = $@"
.execute database script with (ContinueOnErrors=false, ThrowOnErrors=true) <|
{string.Join(Environment.NewLine, commands)}";

            await _queuedClient.ExecuteCommandAsync(
                priority,
                DbStatus.DbName,
                commandText,
                r => r);
        }

        private IEnumerable<string> GetTableSchemaCommands(
            string tableName,
            bool doesTableExist,
            IImmutableList<TableColumn> schema)
        {
            var columnListText = string.Join(
                ", ",
                schema.Select(i => $"{i.Name}:{i.Type}"));

            if (doesTableExist)
            {
                var tableAlter =
                    $@".alter table['{tableName}']({columnListText})";

                return new[] { tableAlter };
            }
            else
            {
                var tableCreate =
                    $@".create table['{tableName}']({columnListText})";
                var alterTableRetention = $@".alter table ['{tableName}'] policy retention
```
{{
    ""SoftDeletePeriod"": ""40000.00:00:00""
}}
```";

                return new[] { tableCreate, alterTableRetention };
            }
        }

        private async Task<IImmutableSet<string>> ExistingTablesAsync(
            IEnumerable<string> tableNames,
            KustoPriority priority)
        {
            var tableListText = string.Join(", ", tableNames.Select(t => $"'{t}'"));
            var commandText = $@"
.show tables
| where TableName in ({tableListText})
| project TableName
";
            var existingTables = await _queuedClient.ExecuteCommandAsync(
                priority,
                DbStatus.DbName,
                commandText,
                r => (string)r["TableName"]);

            return existingTables.ToImmutableHashSet();
        }
        #endregion

        #region Move Extents
        private async Task MoveAllExtentsAsync(StatusItem subIteration)
        {
            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var moveTasks = DbStatus
                .GetRecordBatches(subIterationKey.IterationId, subIterationKey.SubIterationId)
                .GroupBy(r => r.TableName)
                .Select(g => MoveTableExtentsAsync(g.ToImmutableArray(), subIteration))
                .ToImmutableArray();

            await Task.WhenAll(moveTasks);
        }

        private async Task MoveTableExtentsAsync(
            ImmutableArray<StatusItem> recordBatchItems,
            StatusItem subIteration)
        {
            var subIterationKey = SubIterationKey.FromSubIteration(subIteration);
            var tableName = recordBatchItems.First().TableName;
            var stagingTableName = recordBatchItems.First().GetStagingTableName(subIteration);
            var extentIds = recordBatchItems
                .SelectMany(r => r.InternalState.RecordBatchState!.StageRecordBatchState!.ExtentIds)
                .Distinct();
            var priority = new KustoPriority(
               subIterationKey.IterationId,
               subIterationKey.SubIterationId,
               DbStatus.DbName,
               stagingTableName);
            var commandText = $@".move extents
from table ['{stagingTableName}']
to table ['{tableName}']
({string.Join(", ", extentIds)})";

            await _queuedClient.ExecuteCommandAsync(
                priority,
                DbStatus.DbName,
                commandText,
                r => r);
        }
        #endregion
    }
}