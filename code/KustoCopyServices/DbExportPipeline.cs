﻿using Azure.Core;
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
            
            public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
        }
        #endregion

        private readonly DbExportBookmark _dbExportBookmark;
        private readonly KustoClient _kustoClient;
        private readonly ITempFolderService _tempFolderService;

        private DbExportPipeline(
            string dbName,
            DbExportBookmark dbExportBookmark,
            KustoClient kustoClient,
            ITempFolderService tempFolderService)
        {
            DbName = dbName;
            _dbExportBookmark = dbExportBookmark;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
        }

        public static async Task<DbExportPipeline> CreateAsync(
            string dbName,
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            ITempFolderService tempFolderService)
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
                tempFolderService);
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

            var nextDayTables = _dbExportBookmark.GetNextDayTables(isBackfill);

            if (!nextDayTables.Any())
            {
                throw new NotImplementedException("Copy is done");
            }
            else
            {
                var copyTableTasks = nextDayTables
                    .Select(t => CopyDayTableAsync(isBackfill, t))
                    .ToImmutableArray();

                await Task.WhenAll(copyTableTasks);
            }
        }

        private async Task CopyDayTableAsync(bool isBackfill, string tableName)
        {
            var tableIteration = _dbExportBookmark.GetTableIterationData(tableName, isBackfill);
            var dayInterval = tableIteration.GetNextDayInterval(isBackfill);
            var exportPlan = ConstructExportPlanAsync(tableName, dayInterval, isBackfill);

            await ValueTask.CompletedTask;
        }

        private async Task ConstructExportPlanAsync(
            string tableName,
            (DateTime Min, DateTime Max) dayInterval,
            bool isBackfill)
        {
            var maxInequality = isBackfill ? "<" : "<=";
            var ingestionTimes = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .SetParameter("DayIntervalMin", dayInterval.Min)
                .SetParameter("DayIntervalMax", dayInterval.Max)
                .ExecuteQueryAsync(
                DbName,
                @$"
declare query_parameters(TargetTableName: string);
declare query_parameters(DayIntervalMin: datetime);
declare query_parameters(DayIntervalMax: datetime);
table(TargetTableName)
| where ingestion_time() >= DayIntervalMin
| where ingestion_time() {maxInequality} DayIntervalMax
| summarize by IngestionTime=ingestion_time(), ExtentId=extent_id()
| order by IngestionTime asc
| limit 1000
",
                r => new
                {
                    IngestionTime = (DateTime)r["IngestionTime"],
                    ExtentId = (string)r["ExtentId"]
                });

            throw new NotImplementedException();
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