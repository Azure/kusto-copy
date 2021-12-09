using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Export;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class ExportPipeline : IExportPipeline
    {
        private readonly ExportBookmark _exportBookmark;
        private readonly KustoClient _kustoClient;
        private readonly ITempFolderService _tempFolderService;

        private ExportPipeline(
            ExportBookmark exportBookmark,
            KustoClient kustoClient,
            ITempFolderService tempFolderService)
        {
            _exportBookmark = exportBookmark;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
        }

        public static async Task<ExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            TempFolderService tempFolderService)
        {
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");
            var exportBookmark = await ExportBookmark.RetrieveAsync(
                sourceFolderClient.GetFileClient("export.bookmark"),
                credential);

            return new ExportPipeline(exportBookmark, kustoClient, tempFolderService);
        }

        public async Task RunAsync()
        {
            await InitExportAsync();
        }

        private async Task InitExportAsync()
        {
            if (_exportBookmark.IsBackfill == null)
            {
                using (var tempFolder = _tempFolderService.LeaseTempFolder())
                {   //  Fetch the databases from the cluster
                    var databaseNames = await _kustoClient.ExecuteCommandAsync(
                        string.Empty,
                        ".show databases | project DatabaseName",
                        r => (string)r["DatabaseName"]);
                    //  Snapshot the current cursor for each database
                    var cursorTasks = databaseNames
                        .Select(db => _kustoClient.ExecuteQueryAsync(
                            db,
                            "print Cursor=cursor_current()",
                            r => (string)r["Cursor"]))
                        .ToImmutableArray();

                    await Task.WhenAll(cursorTasks);

                    var dbCursors = databaseNames
                        .Zip(cursorTasks, (db, t) => (db, cursor: t.Result.First()));
//                    .show tables | project TableName

//Diagnostics
//| summarize Min = min(ingestion_time()), Max = max(ingestion_time())
//// | where cursor_before_or_at("")
                }

                await Task.CompletedTask;
            }
        }
    }
}