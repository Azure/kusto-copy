using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyFoundation;
using KustoCopyFoundation.Concurrency;
using KustoCopyFoundation.KustoQuery;
using KustoCopySpecific.Bookmarks.Common;
using KustoCopySpecific.Bookmarks.DbStorage;
using KustoCopySpecific.Bookmarks.ExportPlan;
using KustoCopySpecific.Bookmarks.IterationExportStorage;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Intrinsics.Arm;

namespace KustoCopySpecific.Pipelines
{
    public class DbExportExecutionPipeline
    {
        #region Inner Types
        public class TablePlanCountDown
        {
            private volatile int _tableCount;

            public TablePlanCountDown(int tableCount)
            {
                _tableCount = tableCount;
            }

            public bool IsIterationCompletelyExported => _tableCount == 0;

            public void DecreaseTableCount()
            {
                Interlocked.Decrement(ref _tableCount);
            }
        }

        public class TablePlanContext : IComparable<TablePlanContext>
        {
            public TablePlanContext(
                TablePlanCountDown tablePlanCountDown,
                DbEpochData dbEpoch,
                DbIterationData dbIteration,
                TableExportPlanData tablePlan)
            {
                TablePlanCountDown = tablePlanCountDown;
                DbEpoch = dbEpoch;
                DbIteration = dbIteration;
                TablePlan = tablePlan;
            }

            public TablePlanCountDown TablePlanCountDown { get; }

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

        private readonly DataLakeDirectoryClient _rootTempFolderClient;
        private readonly DbStorageBookmark _dbStorageBookmark;
        private readonly DbIterationStorageFederation _iterationFederation;
        private readonly KustoQueuedClient _kustoClient;
        private readonly KustoExportQueue _exportQueue;
        private readonly KustoOperationAwaiter _operationAwaiter;
        private readonly PriorityQueue<TablePlanContext, TablePlanContext> _planQueue
            = new PriorityQueue<TablePlanContext, TablePlanContext>();

        public DbExportExecutionPipeline(
            DataLakeDirectoryClient rootTempFolderClient,
            string dbName,
            DbStorageBookmark dbStorageBookmark,
            DbExportPlanBookmark dbExportPlanBookmark,
            DbIterationStorageFederation iterationFederation,
            KustoQueuedClient kustoClient,
            int concurrentExportCommandCount)
        {
            _rootTempFolderClient = rootTempFolderClient;
            DbName = dbName;
            DbExportPlanBookmark = dbExportPlanBookmark;
            _dbStorageBookmark = dbStorageBookmark;
            _iterationFederation = iterationFederation;
            _kustoClient = kustoClient;
            _exportQueue = new KustoExportQueue(_kustoClient, concurrentExportCommandCount);
            _operationAwaiter = new KustoOperationAwaiter(_kustoClient, DbName);
            //  Populate plan queue
            var list = new List<TablePlanContext>();

            foreach (var dbEpoch in DbExportPlanBookmark.GetAllDbEpochs())
            {
                foreach (var dbIteration in DbExportPlanBookmark.GetDbIterations(dbEpoch.EndCursor))
                {
                    var plans = DbExportPlanBookmark.GetTableExportPlans(
                        dbIteration.EpochEndCursor,
                        dbIteration.Iteration);
                    var planCountDown = new TablePlanCountDown(plans.Count);

                    foreach (var tablePlan in plans)
                    {
                        var context = new TablePlanContext(
                            planCountDown,
                            dbEpoch,
                            dbIteration,
                            tablePlan);

                        list.Add(context);
                    }
                }
            }
            _planQueue.EnqueueRange(list.Select(c => (c, c)));
            DbExportPlanBookmark.NewDbIteration += (sender, e) =>
            {
                lock (_planQueue)
                {
                    var planCountDown = new TablePlanCountDown(e.TablePlans.Count);
                    var contexts = e.TablePlans
                    .Select(p => new TablePlanContext(planCountDown, e.DbEpoch, e.DbIteration, p))
                    .Select(c => (c, c));

                    _planQueue.EnqueueRange(contexts);
                }
            };
        }

        public string DbName { get; }

        public DbExportPlanBookmark DbExportPlanBookmark { get; }

        public async Task RunAsync()
        {
            var stopGoAwaiter = new StopGoAwaiter(false);
            var taskList = new List<Task>();

            DbExportPlanBookmark.NewDbIteration += (sender, e) =>
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
                    taskList.Add(ProcessTablePlanAsync(context));
                }
            }
        }

        /// <summary>
        /// To make the plan processing transactional:
        /// 
        /// 1 - Snapshot schema
        /// 2 - Export to temp folder
        /// 3 - Snapshot schema (both must be same or redo)
        /// 4 - Bookmark as stored (and mark iteration as completed if that was the last table)
        /// 5 - Move folder
        /// 6 - Raise notification for subscription (destination clusters)
        /// 7 - Remove the plan bookmark
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        private async Task ProcessTablePlanAsync(TablePlanContext context)
        {
            var tableMessage = $"database '{DbName}', table '{context.TablePlan.TableName}' "
                + $"in iteration "
                + $"{context.DbIteration.Iteration} of epoch {context.DbEpoch.EpochStartTime}";
            var watch = new Stopwatch();

            Trace.TraceInformation($"Start export for {tableMessage}");
            watch.Start();

            var iterationFolderClient = _iterationFederation.GetIterationFolder(
                context.DbEpoch.IsBackfill,
                context.DbEpoch.EpochStartTime,
                context.DbIteration.Iteration);
            var tempFolderClient = _rootTempFolderClient.GetSubDirectoryClient(
                $"{context.TablePlan.TableName}-{Guid.NewGuid()}");
            var tableFolderClient =
                iterationFolderClient.GetSubDirectoryClient(context.TablePlan.TableName);
            var tableFolderExistTask = tableFolderClient.ExistsAsync();
            var iterationBookmark = await _iterationFederation.FetchIterationBookmarkAsync(
                context.DbEpoch.IsBackfill,
                context.DbEpoch.EpochStartTime,
                context.DbIteration.Iteration);
            var tableFolderExist = (await tableFolderExistTask).Value;
            var table = iterationBookmark.GetTable(context.TablePlan.TableName);

            //  It is possible the table was processed but the plan wasn't removed
            //  It is also possible the table was processed but temp folder wasn't moved
            //  In the latter case, we reprocess the table
            if (table == null || tableFolderExist)
            {
                var schemaBefore = await FetchTableSchemaAsync(context.TablePlan.TableName);
                var deleteExistingFolderTask = tableFolderExist
                    ? tableFolderClient.DeleteAsync()
                    : Task.CompletedTask;

                table = new TableStorageData
                {
                    TableName = context.TablePlan.TableName,
                    Schema = schemaBefore
                };
                if (context.TablePlan.Steps.Any())
                {
                    var stepIndexes = Enumerable.Range(0, context.TablePlan.Steps.Count());
                    var stepTasks = context.TablePlan.Steps
                        .Zip(stepIndexes, (s, i) => (s, i))
                        .Select(c => ProcessStepAsync(context, tempFolderClient, c.s, c.i))
                        .ToImmutableArray();

                    await Task.WhenAll(stepTasks);
                    table.Steps = stepTasks.Select(t => t.Result).ToImmutableArray();

                    var schemaAfter = await FetchTableSchemaAsync(context.TablePlan.TableName);

                    if (!schemaBefore.Equals(schemaAfter))
                    {   //  Schema changed while we were processing:  cleanup and redo
                        var cleanupTask = tempFolderClient.DeleteAsync();
                        var reprocessTask = ProcessTablePlanAsync(context);

                        await Task.WhenAll(cleanupTask, reprocessTask);
                        Trace.WriteLine($"Schema mismatch for {tableMessage} ; redo");

                        return;
                    }

                    await deleteExistingFolderTask;
                    //  Move temp folder to permanent location
                    await tempFolderClient.RenameAsync(tableFolderClient.Path);
                }
            }
            context.TablePlanCountDown.DecreaseTableCount();
            await iterationBookmark.CreateTableAsync(
                table,
                tableFolderExist,
                context.TablePlanCountDown.IsIterationCompletelyExported);
            //  This is done on a different blob, hence a different "transaction"
            //  For that reason it might fail in between hence the check for table not null
            await DbExportPlanBookmark.CompleteTableExportPlanAsync(context.TablePlan);
            Trace.TraceInformation($"Export for {tableMessage} done:  {watch.Elapsed}");
        }

        private async Task<TableStorageFolderData> ProcessStepAsync(
            TablePlanContext context,
            DataLakeDirectoryClient tableFolderClient,
            TableExportStepData step,
            int stepIndex)
        {
            var results = await _exportQueue.RequestRunAsync(async () =>
            {
                var operationId = await ExportAsync(
                    context.TablePlan.TableName,
                    stepIndex,
                    (context.DbEpoch.StartCursor, context.DbEpoch.EndCursor),
                    step.IngestionTimes,
                    tableFolderClient);

                await _operationAwaiter.WaitForOperationCompletionAsync(operationId);

                var results = await _kustoClient.ExecuteCommandAsync(
                    KustoPriority.ExportPriority,
                    DbName,
                    $".show operation {operationId} details",
                    r => new
                    {
                        Path = (string)r["Path"],
                        NumRecords = (long)r["NumRecords"]
                    });

                return results;
            });

            return new TableStorageFolderData
            {
                OverrideIngestionTime = step.OverrideIngestionTime,
                RowCount = results.Sum(r => r.NumRecords),
                BlobNames = results.Select(r => Path.GetFileName(r.Path)).ToImmutableArray()
            };
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
            var prefix = $"{tableName}-{stepIndex:0000}";
            var commandText = @$"
.export async compressed
to csv (h@'{folderClient.Uri};impersonate')
with(namePrefix = '{prefix}', includeHeaders = all, encoding = UTF8NoBOM) <|
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