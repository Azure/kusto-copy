using Azure.Core;
using Azure.Storage.Files.DataLake;
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

        private static async Task<(string, IImmutableList<TableBookmark>)> FetchDefaultBookmarks(
            string dbName,
            KustoClient kustoClient)
        {
            var tableNamesTask = kustoClient.ExecuteCommandAsync(
                dbName,
                ".show tables | project TableName",
                r => (string)r["TableName"]);
            var cursors = await kustoClient.ExecuteQueryAsync(
                dbName,
                "print Cursor=cursor_current()",
                r => (string)r["Cursor"]);
            var tableNames = await tableNamesTask;
            var latestCursor = cursors.First();
            var tableBookmarks = await FetchTableBookmarksAsync(
                dbName,
                kustoClient,
                latestCursor,
                tableNames,
                DEFAULT_FETCH_TABLES_SIZE);

            return (latestCursor, tableBookmarks);
        }

        private static async Task<ImmutableArray<TableBookmark>> FetchTableBookmarksAsync(
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

        private static async Task<ImmutableArray<TableBookmark>> FetchTableChunkBookmarksAsync(
            string dbName,
            KustoClient kustoClient,
            string latestCursor,
            IImmutableList<string> tableNames)
        {
            var commandHeader = @"
declare query_parameters(Cursor:string);
let fetchRange = (tableName:string) {
    table(tableName)
    | where cursor_before_or_at(Cursor)
    | summarize Min = min(ingestion_time()), Max = max(ingestion_time())
    | extend TableName = tableName
};
";
            var tableCommandlets = tableNames.Select(t => $"fetchRange('{t}')");
            var commandText = commandHeader + string.Join(" | union ", tableCommandlets);
            var bookmarks = await kustoClient.SetParameter("Cursor", latestCursor).ExecuteQueryAsync(
                dbName,
                commandText,
                r => new TableBookmark
                {
                    TableName = (string)r["TableName"],
                    IsBackfill = true,
                    MinTime = r["Min"].To<DateTime>(),
                    MaxTime = r["Max"].To<DateTime>(),
                    RemainingMinTime = r["Min"].To<DateTime>(),
                    RemainingMaxTime = r["Min"].To<DateTime>()
                });

            return bookmarks;
        }
    }
}