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
    public class ClusterExportPipeline : IExportPipeline
    {
        private readonly DataLakeDirectoryClient _sourceFolderClient;
        private readonly TokenCredential _credential;
        private readonly KustoClient _kustoClient;
        private readonly ITempFolderService _tempFolderService;
        private readonly IDictionary<string, DbExportPipeline> _dbExportPipelineMap =
            new Dictionary<string, DbExportPipeline>();

        private ClusterExportPipeline(
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            ITempFolderService tempFolderService)
        {
            _sourceFolderClient = sourceFolderClient;
            _credential = credential;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
        }

        public static async Task<ClusterExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            TempFolderService tempFolderService)
        {
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");

            await ValueTask.CompletedTask;

            return new ClusterExportPipeline(
                sourceFolderClient,
                credential,
                kustoClient,
                tempFolderService);
        }

        public async Task RunAsync()
        {
            await SyncDbListAsync();
        }

        private async Task SyncDbListAsync()
        {
            //  Fetch the database list from the cluster
            var nextDbNames = await _kustoClient.ExecuteCommandAsync(
                string.Empty,
                ".show databases | project DatabaseName",
                r => (string)r["DatabaseName"]);
            var currentDbNames = _dbExportPipelineMap.Keys.ToImmutableArray();
            var obsoleteDbNames = currentDbNames.Except(nextDbNames);
            var newDbNames = nextDbNames.Except(currentDbNames);
            var newDbExportPipelineTasks = newDbNames
                .Select(db => DbExportPipeline.CreateAsync(
                    db,
                    _sourceFolderClient.GetSubDirectoryClient(db),
                    _credential,
                    _kustoClient,
                    _tempFolderService))
                .ToImmutableArray();

            await Task.WhenAll(newDbExportPipelineTasks);
            foreach (var db in obsoleteDbNames)
            {
                _dbExportPipelineMap.Remove(db);
            }
            foreach (var dbPipeline in newDbExportPipelineTasks.Select(t => t.Result))
            {
                _dbExportPipelineMap.Add(dbPipeline.DbName, dbPipeline);
            }
        }
    }
}