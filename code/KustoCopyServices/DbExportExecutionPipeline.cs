using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.ExportPlan;
using KustoCopyBookmarks.ExportStorage;
using KustoCopyBookmarks.Parameters;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Intrinsics.Arm;

namespace KustoCopyServices
{
    internal class DbExportExecutionPipeline
    {
        #region Inner Types
        public class TablePlanContext : IComparable<TablePlanContext>
        {
            public TablePlanContext(
                DbEpochData dbEpoch,
                DbIterationData dbIteration,
                TableExportPlanData tablePlan)
            {
                DbEpoch = dbEpoch;
                DbIteration = dbIteration;
                TablePlan = tablePlan;
            }

            public DbEpochData DbEpoch { get; }

            public DbIterationData DbIteration { get; }

            public TableExportPlanData TablePlan { get; }

            public bool IsEmpty => !TablePlan.Steps.Any();

            int IComparable<TablePlanContext>.CompareTo(TablePlanContext? other)
            {
                if (other == null)
                {
                    throw new ArgumentNullException(nameof(other));
                }

                if (IsEmpty && other.IsEmpty)
                {
                    return 0;
                }
                else if (IsEmpty && !other.IsEmpty)
                {
                    return -1;
                }
                else if (!IsEmpty && other.IsEmpty)
                {
                    return 1;
                }
                else
                {
                    var thisIngestionTime = TablePlan.Steps.First().IngestionTimes.Max();
                    var otherIngestionTime = other.TablePlan.Steps.First().IngestionTimes.Max();

                    return thisIngestionTime.CompareTo(otherIngestionTime);
                }
            }
        }
        #endregion

        private readonly DbExportPlanBookmark _dbExportPlanBookmark;
        private readonly DbIterationStorageFederation _iterationFederation;
        private readonly KustoQueuedClient _kustoClient;
        private readonly KustoExportQueue _exportQueue;
        private readonly KustoOperationAwaiter _operationAwaiter;
        private readonly PriorityQueue<TablePlanContext, TablePlanContext> _planQueue
            = new PriorityQueue<TablePlanContext, TablePlanContext>();

        public DbExportExecutionPipeline(
            string dbName,
            DbExportPlanBookmark dbExportPlanBookmark,
            DbIterationStorageFederation iterationFederation,
            KustoQueuedClient kustoClient,
            double exportSlotsRatio)
        {
            DbName = dbName;
            _dbExportPlanBookmark = dbExportPlanBookmark;
            _iterationFederation = iterationFederation;
            _kustoClient = kustoClient;
            _exportQueue = new KustoExportQueue(_kustoClient, exportSlotsRatio);
            _operationAwaiter = new KustoOperationAwaiter(_kustoClient, DbName);
            //  Populate plan queue
            var list = new List<TablePlanContext>();

            foreach (var dbEpoch in _dbExportPlanBookmark.GetAllDbEpochs())
            {
                foreach (var dbIteration in _dbExportPlanBookmark.GetDbIterations(dbEpoch.EndCursor))
                {
                    var plans = _dbExportPlanBookmark.GetTableExportPlans(
                        dbIteration.EpochEndCursor,
                        dbIteration.Iteration);

                    foreach (var tablePlan in plans)
                    {
                        var context = new TablePlanContext(dbEpoch, dbIteration, tablePlan);

                        list.Add(context);
                    }
                }
            }
            _planQueue.EnqueueRange(list.Select(c => (c, c)));
            _dbExportPlanBookmark.NewDbIteration += (sender, e) =>
            {
                lock (_planQueue)
                {
                    var contexts = e.TablePlans
                    .Select(p => new TablePlanContext(e.DbEpoch, e.DbIteration, p))
                    .Select(c => (c, c));

                    _planQueue.EnqueueRange(contexts);
                }
            };
        }

        public string DbName { get; }

        public async Task RunAsync()
        {
            var stopGoAwaiter = new StopGoAwaiter(false);
            var taskList = new List<Task>();

            _dbExportPlanBookmark.NewDbIteration += (sender, e) =>
            {   //  Make sure we wake up
                stopGoAwaiter.Go();
            };

            while (true)
            {
                TablePlanContext? context;

                lock (_planQueue)
                {
                    if (!_planQueue.TryDequeue(out context, out _))
                    {
                        stopGoAwaiter.Stop();
                    }
                }
                if (context == null)
                {
                    await stopGoAwaiter.WaitForGoAsync();
                }
                else
                {
                    while (!_exportQueue.HasAvailability)
                    {
                        taskList = await CleanTaskListAsync(taskList);
                    }
                    taskList.Add(ProcessPlanAsync(context));
                }
            }
        }

        private async Task ProcessPlanAsync(TablePlanContext context)
        {
            var iterationFolderClient = _iterationFederation.GetIterationFolder(
                context.DbEpoch.IsBackfill,
                context.DbEpoch.EpochStartTime,
                context.DbIteration.Iteration);
            var tableFolderClient =
                iterationFolderClient.GetSubDirectoryClient(context.TablePlan.TableName);
            var iterationBookmark = await _iterationFederation.FetchIterationBookmarkAsync(
                context.DbEpoch.IsBackfill,
                context.DbEpoch.EpochStartTime,
                context.DbIteration.Iteration);
            var table = iterationBookmark.GetTable(context.TablePlan.TableName);

            //  It is possible the table was processed but the plan wasn't removed
            if (table == null)
            {
                var schemaBefore = await FetchTableSchemaAsync(context.TablePlan.TableName);

                table = new TableStorageData
                {
                    TableName = context.TablePlan.TableName,
                    Schema = schemaBefore
                };
                if (context.TablePlan.Steps.Any())
                {
                    await CleanUpFolderAsync(tableFolderClient);

                    var stepIndexes = Enumerable.Range(0, context.TablePlan.Steps.Count());
                    var stepTasks = context.TablePlan.Steps
                        .Zip(stepIndexes, (s, i) => (s, i))
                        .Select(c => ProcessStepAsync(context, tableFolderClient, c.s, c.i))
                        .ToImmutableArray();

                    await Task.WhenAll(stepTasks);
                }
            }
            //  This is done on a different blob, hence a different "transaction"
            //  For that reason it might fail in between hence the check for table not null
            await _dbExportPlanBookmark.CompleteTableExportPlanAsync(context.TablePlan);
        }

        private async Task ProcessStepAsync(
            TablePlanContext context,
            DataLakeDirectoryClient tableFolderClient,
            TableExportStepData step,
            int stepIndex)
        {
            var operationId = await _exportQueue.RequestRunAsync(async () => await ExportAsync(
                context.TablePlan.TableName,
                stepIndex,
                (context.DbEpoch.StartCursor, context.DbEpoch.EndCursor),
                step.IngestionTimes,
                tableFolderClient));

            await _operationAwaiter.WaitForOperationCompletionAsync(operationId);
        }

        private async Task<Guid> ExportAsync(
            string tableName,
            int stepIndex,
            (string? startCursor, string endCursor) cursorInterval,
            IEnumerable<DateTime> ingestionTimes,
            DataLakeDirectoryClient folderClient)
        {
            var ingestionTimeList = string.Join(
                ", ",
                ingestionTimes.Select(t => CslDateTimeLiteral.AsCslString(t)));
            var commandText = @$"
.export async compressed
to csv (h@'{folderClient.Uri};impersonate')
with(namePrefix = 'export-{stepIndex:0000}', includeHeaders = all, encoding = UTF8NoBOM) <|
['{tableName}']
| where cursor_before_or_at('{cursorInterval.endCursor}')
| where cursor_after('{cursorInterval.startCursor}')
| where ingestion_time() in ({ingestionTimeList})
";
            var operationIds = await _kustoClient
                .ExecuteCommandAsync(
                KustoPriority.ExportPriority,
                DbName,
                commandText,
                r => (Guid)r["OperationId"]);
            var operationId = operationIds.First();

            return operationId;
        }

        private async Task CleanUpFolderAsync(DataLakeDirectoryClient folderClient)
        {
            if (await folderClient.ExistsAsync())
            {
                var paths = await folderClient.GetPathsAsync().ToListAsync();
                var exportBlobs = paths.Where(p => !p.Name.EndsWith(".bookmark"));
                var deleteTasks = exportBlobs
                    .Select(p => folderClient.GetFileClient(p.Name).DeleteAsync());

                await Task.WhenAll(deleteTasks);
            }
        }

        private async Task<TableSchemaData> FetchTableSchemaAsync(string tableName)
        {
            //  Technically we could parse the 'Schema' column but in general it would require
            //  taking care of character escape which make it non-trivial so we use getschema
            //  in a separate query
            var tableSchemaTask = _kustoClient.ExecuteCommandAsync(
                KustoPriority.ExportPriority,
                DbName,
                $".show table ['{tableName}'] schema as csl | project Folder, DocString",
                r => new TableSchemaData
                {
                    Folder = (string)r["Folder"],
                    DocString = (string)r["DocString"]
                });
            var columns = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .ExecuteQueryAsync(
                KustoPriority.ExportPriority,
                DbName,
                "declare query_parameters(TargetTableName: string);"
                + "table(TargetTableName) | getschema | project ColumnName, ColumnType",
                r => new ColumnSchemaData
                {
                    ColumnName = (string)r["ColumnName"],
                    ColumnType = (string)r["ColumnType"]
                });
            var tableSchema = (await tableSchemaTask).FirstOrDefault();

            if (tableSchema == null)
            {
                throw new CopyException($"Table '{tableName}' was dropped during export");
            }

            tableSchema.Columns = columns;

            return tableSchema;
        }

        private static async Task<List<Task>> CleanTaskListAsync(List<Task> taskList)
        {
            await Task.WhenAny(taskList);

            //  We take a snapshot of the state
            var snapshot = taskList
                .Select(t => new
                {
                    t.IsCompleted,
                    Task = t
                })
                .ToImmutableArray();
            var completed = snapshot
                .Where(s => s.IsCompleted);

            foreach (var s in completed)
            {
                await s.Task;
            }

            return snapshot
                .Where(s => !s.IsCompleted)
                .Select(s => s.Task)
                .ToList();
        }
    }
}