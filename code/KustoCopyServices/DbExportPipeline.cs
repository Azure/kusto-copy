using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Export;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyServices
{
    internal class DbExportPipeline
    {
        #region Inner Types
        private class ExportPlan
        {
            public DateTime IngestionTime { get; set; } = DateTime.MinValue;

            public Guid ExtentId { get; set; } = Guid.Empty;

            public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
        }

        private class IngestionSchedule
        {
            public DateTime IngestionTime { get; set; } = DateTime.MinValue;

            public Guid ExtentId { get; set; } = Guid.Empty;
        }

        private class ExtentConfiguration
        {
            public Guid ExtentId { get; set; } = Guid.Empty;

            public DateTime MinCreatedOn { get; set; } = DateTime.MinValue;
        }

        private class ExportResult
        {
            public string Path { get; set; } = string.Empty;

            public long NumRecords { get; set; } = 0;
        }
        #endregion

        private readonly DbExportBookmark _dbExportBookmark;
        private readonly KustoClient _kustoClient;
        private readonly TempFolderService _tempFolderService;
        private readonly KustoExportQueue _exportQueue;
        private readonly KustoOperationAwaiter _operationAwaiter;

        private DbExportPipeline(
            string dbName,
            DbExportBookmark dbExportBookmark,
            KustoClient kustoClient,
            TempFolderService tempFolderService,
            KustoExportQueue exportQueue)
        {
            DbName = dbName;
            _dbExportBookmark = dbExportBookmark;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
            _exportQueue = exportQueue;
            _operationAwaiter = new KustoOperationAwaiter(_kustoClient, dbName);
        }

        public static async Task<DbExportPipeline> CreateAsync(
            string dbName,
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            TempFolderService tempFolderService,
            KustoExportQueue exportQueue)
        {
            var dbExportBookmark = await DbExportBookmark.RetrieveAsync(
                sourceFolderClient.GetFileClient("source-db.bookmark"),
                credential,
                async () =>
                {
                    return await FetchDefaultBookmarks(dbName, kustoClient);
                });

            return new DbExportPipeline(
                dbName,
                dbExportBookmark,
                kustoClient,
                tempFolderService,
                exportQueue);
        }

        public string DbName { get; }

        public async Task RunAsync()
        {
            var backfillTask = CopyAsync(true);
            var currentTask = OrchestrateForwardCopyAsync();

            await Task.WhenAll(backfillTask, currentTask);
        }

        private async Task CopyAsync(bool isBackfill)
        {
            await ProcessEmptyIngestionTableAsync(isBackfill);

            var cursorInterval = _dbExportBookmark.GetCursorInterval(isBackfill);
            var nextDayTables = _dbExportBookmark.GetNextDayTables(isBackfill);

            if (!nextDayTables.Any())
            {
                throw new NotSupportedException("Copy is done");
            }
            else
            {
                var copyTableTasks = nextDayTables
                    .Select(t => CopyDayTableAsync(isBackfill, cursorInterval, t))
                    .ToImmutableArray();

                await Task.WhenAll(copyTableTasks);
            }
        }

        private async Task CopyDayTableAsync(
            bool isBackfill,
            (string? startCursor, string endCursor) cursorInterval,
            string tableName)
        {
            var tableIteration = _dbExportBookmark.GetTableIterationData(tableName, isBackfill);
            var dayInterval = tableIteration.GetNextDayInterval(isBackfill);
            var exportPlan = await ConstructExportPlanAsync(
                tableName,
                cursorInterval,
                dayInterval,
                isBackfill);
            var groups = exportPlan.GroupBy(e => e.ExtentId);
            var exportPlanTasks = groups
                .Select(g => ExecuteExportPlansAsync(tableName, cursorInterval, g))
                .ToImmutableArray();

            await Task.WhenAll(exportPlanTasks);
        }

        private async Task ExecuteExportPlansAsync(
            string tableName,
            (string? startCursor, string endCursor) cursorInterval,
            IEnumerable<ExportPlan> plans)
        {
            var ingestionTimes = plans.Select(p => p.IngestionTime);

            await _exportQueue.RequestRunAsync(
                DbName,
                tableName,
                cursorInterval.startCursor,
                cursorInterval.endCursor,
                ingestionTimes.First());

            var tempFolderLease = _tempFolderService.LeaseTempFolder();

            try
            {
                var operationId =
                    await ExportAsync(tableName, cursorInterval, ingestionTimes, tempFolderLease);

                await _operationAwaiter.WaitForOperationCompletionAsync(operationId);

                var results = await FetchExportResultsAsync(operationId);
                var names = await tempFolderLease.Client.GetPathsAsync().ToListAsync(p => p.Name);

                throw new NotImplementedException();
            }
            catch
            {
                tempFolderLease.Dispose();
            }
        }

        private async Task<IImmutableList<ExportResult>> FetchExportResultsAsync(Guid operationId)
        {
            var results = await _kustoClient
                .ExecuteCommandAsync(
                DbName,
                $".show operation {operationId} details",
                r => new ExportResult
                {
                    Path = (string)r["Path"],
                    NumRecords = (long)r["NumRecords"]
                });

            return results;
        }

        private async Task<Guid> ExportAsync(
            string tableName,
            (string? startCursor, string endCursor) cursorInterval,
            IEnumerable<DateTime> ingestionTimes,
            ITempFolderLease tempFolderLease)
        {
            var ingestionTimeList = string.Join(
                ", ",
                ingestionTimes.Select(t => CslDateTimeLiteral.AsCslString(t)));
            var commandText = @$"
.export async compressed
to csv (h@'{tempFolderLease.Client.Uri};impersonate')
with(namePrefix = 'export', includeHeaders = all, encoding = UTF8NoBOM) <|
['{tableName}']
| where cursor_before_or_at('{cursorInterval.endCursor}')
| where cursor_after('{cursorInterval.startCursor}')
| where ingestion_time() in ({ingestionTimeList})
";
            var operationIds = await _kustoClient
                .ExecuteCommandAsync(
                DbName,
                commandText,
                r => (Guid)r["OperationId"]);
            var operationId = operationIds.First();

            return operationId;
        }

        private async Task<IImmutableList<ExportPlan>> ConstructExportPlanAsync(
            string tableName,
            (string? startCursor, string endCursor) cursorInterval,
            (DateTime Min, DateTime Max) dayInterval,
            bool isBackfill)
        {
            var ingestionTimes = await FetchIngestionScheduleAsync(
                tableName,
                cursorInterval,
                dayInterval,
                isBackfill);
            var extentIds = ingestionTimes.Select(i => i.ExtentId).Distinct().ToImmutableArray();
            var extents = await FetchExtentsAsync(tableName, extentIds);

            if (extentIds.Count() != extentIds.Count())
            {   //  Possible if extents got dropped or merged between query & show-command
                //  Fix is simply to re-fetch
                return await ConstructExportPlanAsync(
                    tableName,
                    cursorInterval,
                    dayInterval,
                    isBackfill);
            }
            else
            {
                var extentMap = extents.ToDictionary(e => e.ExtentId, e => e.MinCreatedOn);
                var plans = ingestionTimes
                    .Select(i => new ExportPlan
                    {
                        IngestionTime = i.IngestionTime,
                        ExtentId = i.ExtentId,
                        OverrideIngestionTime = extentMap[i.ExtentId]
                    })
                    .ToImmutableArray();

                return plans;
            }
        }

        private async Task<IImmutableList<ExtentConfiguration>> FetchExtentsAsync(
            string tableName,
            IEnumerable<Guid> extentIds)
        {
            var extentIdList = string.Join(", ", extentIds);
            var commandText = @$"
.show table ['{tableName}'] extents ({extentIdList})
| project ExtentId, MinCreatedOn
";
            var extents = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .ExecuteCommandAsync(
                DbName,
                commandText,
                r => new ExtentConfiguration
                {
                    ExtentId = (Guid)r["ExtentId"],
                    MinCreatedOn = (DateTime)r["MinCreatedOn"]
                });

            return extents;
        }

        private async Task<ImmutableArray<IngestionSchedule>> FetchIngestionScheduleAsync(
            string tableName,
            (string? startCursor, string endCursor) cursorInterval,
            (DateTime Min, DateTime Max) dayInterval,
            bool isBackfill)
        {
            var maxInequality = isBackfill ? "<=" : "<";
            var queryText = @$"
declare query_parameters(TargetTableName: string);
declare query_parameters(DayIntervalMin: datetime);
declare query_parameters(DayIntervalMax: datetime);
declare query_parameters(StartCursor: string);
declare query_parameters(EndCursor: string);
table(TargetTableName)
| where cursor_before_or_at(EndCursor)
| where cursor_after(StartCursor)
| where ingestion_time() >= DayIntervalMin
| where ingestion_time() {maxInequality} DayIntervalMax
| summarize by IngestionTime=ingestion_time(), ExtentId=extent_id()
| order by IngestionTime asc
| limit 1000";
            var ingestionTimes = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .SetParameter("DayIntervalMin", dayInterval.Min)
                .SetParameter("DayIntervalMax", dayInterval.Max)
                .SetParameter("StartCursor", cursorInterval.startCursor ?? string.Empty)
                .SetParameter("EndCursor", cursorInterval.endCursor)
                .ExecuteQueryAsync(
                DbName,
                queryText,
                r => new IngestionSchedule
                {
                    IngestionTime = (DateTime)r["IngestionTime"],
                    ExtentId = (Guid)r["ExtentId"]
                });

            return ingestionTimes;
        }

        private async Task ProcessEmptyIngestionTableAsync(bool isBackfill)
        {
            await _dbExportBookmark.ProcessEmptyTableAsync(
                isBackfill,
                async (tableNames) =>
                {
                    var schemaTasks = tableNames
                    .Select(t => FetchTableSchemaAsync(t))
                    .ToArray();

                    await Task.WhenAll(schemaTasks);

                    return schemaTasks
                    .Select(t => t.Result);
                });
        }

        private async Task<TableSchemaData> FetchTableSchemaAsync(string tableName)
        {
            //  Technically we could parse the 'Schema' column but in general it would require
            //  taking care of character escape which make it non-trivial so we use getschema
            //  in a separate query
            var tableSchemaTask = _kustoClient.ExecuteCommandAsync(
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

        private async Task OrchestrateForwardCopyAsync()
        {
            await ValueTask.CompletedTask;
        }

        private static async Task<(DbIterationData, IImmutableList<TableIterationData>)> FetchDefaultBookmarks(
            string dbName,
            KustoClient kustoClient)
        {
            var tableNamesTask = kustoClient.ExecuteCommandAsync(
                dbName,
                ".show tables | project TableName",
                r => (string)r["TableName"]);
            var iterationInfo = await kustoClient.ExecuteQueryAsync(
                dbName,
                "print CurrentTime=now(), Cursor=cursor_current()",
                r => new
                {
                    CurrentTime = (DateTime)r["CurrentTime"],
                    Cursor = (string)r["Cursor"]
                });
            var tableNames = await tableNamesTask;
            var dbIteration = new DbIterationData
            {
                IterationTime = iterationInfo.First().CurrentTime,
                StartCursor = null,
                EndCursor = iterationInfo.First().Cursor
            };
            var tableIterationTasks = tableNames
                .Select(t => FetchTableIterationAsync(
                    kustoClient,
                    dbName,
                    t,
                    dbIteration.EndCursor))
                .ToImmutableArray();

            await Task.WhenAll(tableIterationTasks);

            var tableIterations = tableIterationTasks
                .Select(t => t.Result)
                .Where(t => t != null)
                .Cast<TableIterationData>()
                .ToImmutableArray();

            return (dbIteration, tableIterations);
        }

        private static async Task<TableIterationData?> FetchTableIterationAsync(
            KustoClient kustoClient,
            string dbName,
            string tableName,
            string endCursor)
        {
            var queryText = @"
declare query_parameters(TargetTable:string);
declare query_parameters(Cursor:string);
table(TargetTable)
| where cursor_before_or_at(Cursor)
| summarize Min=min(ingestion_time()), Max=max(ingestion_time());
";
            var ranges = await kustoClient
                .SetParameter("Cursor", endCursor)
                .SetParameter("TargetTable", tableName)
                .ExecuteQueryAsync(
                dbName,
                queryText,
                r => new
                {
                    Min = r["Min"].To<DateTime>(),
                    Max = r["Max"].To<DateTime>()
                });
            var range = ranges.First();

            Trace.WriteLine($"Setup replication for table '{dbName}'.'{tableName}'");

            return new TableIterationData
            {
                EndCursor = endCursor,
                TableName = tableName,
                MinRemainingIngestionTime = range.Min,
                MaxRemainingIngestionTime = range.Max
            };
        }
    }
}