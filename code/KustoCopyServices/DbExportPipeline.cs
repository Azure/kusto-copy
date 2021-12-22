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
        private const int DEFAULT_FETCH_TABLES_SIZE = 10;

        private readonly DbExportBookmark _exportBookmark;
        private readonly KustoClient _kustoClient;
        private readonly ITempFolderService _tempFolderService;

        private DbExportPipeline(
            string dbName,
            DbExportBookmark exportBookmark,
            KustoClient kustoClient,
            ITempFolderService tempFolderService)
        {
            DbName = dbName;
            _exportBookmark = exportBookmark;
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
            var exportBookmark = await DbExportBookmark.RetrieveAsync(
                sourceFolderClient.GetFileClient("source-db.bookmark"),
                credential,
                async () =>
                {
                    return await FetchDefaultBookmarks(dbName, kustoClient);
                });

            return new DbExportPipeline(
                dbName,
                exportBookmark,
                kustoClient,
                tempFolderService);
        }

        public string DbName { get; }

        public async Task RunAsync()
        {
            var backfillTask = BackfillCopyAsync();
            var currentTask = CurrentCopyAsync();

            await Task.WhenAll(backfillTask, currentTask);
        }

        private async Task BackfillCopyAsync()
        {
            var emptyIngestionTask = ProcessEmptyIngestionTableAsync(true);

            await Task.WhenAll(emptyIngestionTask);
        }

        private async Task ProcessEmptyIngestionTableAsync(bool isBackfill)
        {
            await _exportBookmark.ProcessEmptyTableAsync(
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

        private async Task CurrentCopyAsync()
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
            var iteration = new DbIterationData
            {
                IterationTime = iterationInfo.First().CurrentTime,
                StartCursor = null,
                EndCursor = iterationInfo.First().Cursor
            };
            var tableBookmarks = await FetchTableBookmarksAsync(
                dbName,
                kustoClient,
                iteration.EndCursor,
                tableNames,
                DEFAULT_FETCH_TABLES_SIZE);

            return (iteration, tableBookmarks);
        }

        private static async Task<ImmutableArray<TableIterationData>> FetchTableBookmarksAsync(
            string dbName,
            KustoClient kustoClient,
            string latestCursor,
            IImmutableList<string> tableNames,
            int chunkSize)
        {
            var tableChunks = tableNames.Chunk(chunkSize);
            var bookmarkTasks = tableChunks
                .Select(c => FetchTableChunkBookmarksAsync(
                    dbName,
                    kustoClient,
                    latestCursor,
                    c.ToImmutableArray()))
                .ToImmutableArray();

            await Task.WhenAll(bookmarkTasks);

            var bookmarks = bookmarkTasks
                .SelectMany(t => t.Result)
                .ToImmutableArray();

            return bookmarks;
        }

        private static async Task<ImmutableArray<TableIterationData>> FetchTableChunkBookmarksAsync(
            string dbName,
            KustoClient kustoClient,
            string latestCursor,
            IImmutableList<string> tableNames)
        {
            var queryHeader = @"
declare query_parameters(Cursor:string);
let fetchRange = (tableName:string) {
    table(tableName)
    | where cursor_before_or_at(Cursor) or isnull(ingestion_time())
    | summarize by IngestionDayTime=bin(ingestion_time(), 1d)
    | extend TableName = tableName
};
";
            var tableCommandlets = tableNames.Select(t => $"fetchRange('{t}')");
            var queryText = queryHeader + string.Join(" | union ", tableCommandlets);
            var protoBookmarks = await kustoClient
                .SetParameter("Cursor", latestCursor)
                .ExecuteQueryAsync(
                dbName,
                queryText,
                r => new
                {
                    TableName = (string)r["TableName"],
                    IngestionDayTime = r["IngestionDayTime"].To<DateTime>()
                });
            var noIngestionTimeTables = protoBookmarks
                .Where(p => p.IngestionDayTime == null)
                .Select(p => p.TableName)
                .Distinct()
                .ToHashSet();

            WarningIngestionPolicy(dbName, noIngestionTimeTables);

            var validProtoBookmarks = protoBookmarks
                .Where(p => !noIngestionTimeTables.Contains(p.TableName))
                .Select(p => new
                {
                    TableName = p.TableName,
                    //  MinValue should never occur as we validate before
                    IngestionDayTime = p.IngestionDayTime ?? DateTime.MinValue
                });
            var tableGroups = validProtoBookmarks.GroupBy(p => p.TableName);
            var tableMap = tableGroups.ToImmutableDictionary(g => g.Key);
            var emptyTableBookmarks = tableNames
                .Where(t => !tableMap.ContainsKey(t))
                .Where(t => !noIngestionTimeTables.Contains(t))
                .Select(t => new TableIterationData
                {
                    EndCursor = latestCursor,
                    TableName = t,
                    RemainingDayIngestionTimes = ImmutableArray<DateTime>.Empty
                });
            var nonEmptyTableBookmarks = tableGroups
                .Select(g => new TableIterationData
                {
                    EndCursor = latestCursor,
                    TableName = g.Key,
                    RemainingDayIngestionTimes = g.Select(i => i.IngestionDayTime).ToImmutableArray()
                });
            var bookmarks = emptyTableBookmarks.Concat(nonEmptyTableBookmarks).ToImmutableArray();

            return bookmarks;
        }

        private static void WarningIngestionPolicy(
            string dbName,
            IEnumerable<string> noIngestionTimeTables)
        {
            if (noIngestionTimeTables.Any())
            {
                var tableNameList = string.Join(", ", noIngestionTimeTables.Select(t => $"'{t}'"));

                Trace.TraceWarning(
                    $"Tables {{{tableNameList}}} have entries with no ingestion time and "
                    + "therefore can't be replicated");
            }
        }
    }
}