using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Export;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class ExportPipeline : IExportPipeline
    {
        private readonly ExportBookmark _exportBookmark;
        private readonly ICslAdminProvider _commandProvider;
        private readonly ITempFolderService _tempFolderService;

        private ExportPipeline(
            ExportBookmark exportBookmark,
            ICslAdminProvider commandProvider,
            ITempFolderService tempFolderService)
        {
            _exportBookmark = exportBookmark;
            _commandProvider = commandProvider;
            _tempFolderService = tempFolderService;
        }

        public static async Task<ExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            ICslAdminProvider commandProvider,
            TempFolderService tempFolderService)
        {
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");
            var exportBookmark = await ExportBookmark.RetrieveAsync(
                sourceFolderClient.GetFileClient("export.bookmark"),
                credential);

            return new ExportPipeline(exportBookmark, commandProvider, tempFolderService);
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
                {
                }

                await Task.CompletedTask;
            }
        }
    }
}