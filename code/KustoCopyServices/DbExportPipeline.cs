using Azure.Core;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks.Export;

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
                    var cursors = await kustoClient.ExecuteQueryAsync(
                        dbName,
                        "print Cursor=cursor_current()",
                        r => (string)r["Cursor"]);

                    return cursors.First();
                });
            ////Diagnostics
            ////| summarize Min = min(ingestion_time()), Max = max(ingestion_time())
            ////// | where cursor_before_or_at("")
            //await Task.CompletedTask;

            return new DbExportPipeline(
                dbName,
                exportBookmark,
                kustoClient,
                tempFolderService);
        }

        public string DbName { get; }

    }
}