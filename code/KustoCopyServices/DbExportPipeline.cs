using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Export;
using System.Collections.Immutable;

namespace KustoCopyServices
{
    internal class DbExportPipeline
    {
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
            var tableBookmarkTasks = tableNames
                .Select(t => kustoClient.SetParameter("Cursor", latestCursor).ExecuteQueryAsync(
                    dbName,
                    "declare query_parameters(Cursor:string);"
                    + $"{t} | where cursor_before_or_at(Cursor) "
                    + "| summarize Min = min(ingestion_time()), Max = max(ingestion_time())",
                    r => new TableBookmark
                    {
                        TableName = t,
                        IsBackfill = true,
                        MinTime = r["Min"].To<DateTime>(),
                        MaxTime = r["Max"].To<DateTime>(),
                        ExportedUntilTime = r["Min"].To<DateTime>()
                    }))
                .ToImmutableArray();

            await Task.WhenAll(tableBookmarkTasks);

            var tableBookmarks = tableBookmarkTasks
                .Select(t => t.Result.First())
                .ToImmutableArray();

            return (latestCursor, tableBookmarks);
        }
    }
}