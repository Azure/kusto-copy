using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Export;
using KustoCopyBookmarks.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class ClusterExportPipeline
    {
        private readonly DataLakeDirectoryClient _sourceFolderClient;
        private readonly TokenCredential _credential;
        private readonly KustoClient _kustoClient;
        private readonly TempFolderService _tempFolderService;
        private readonly KustoExportQueue _exportQueue;
        private readonly IDictionary<string, DbExportPipeline> _dbExportPipelineMap =
            new Dictionary<string, DbExportPipeline>();
        private readonly MainParameterization _mainParameterization;

        private ClusterExportPipeline(
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            TempFolderService tempFolderService,
            MainParameterization mainParameterization)
        {
            _sourceFolderClient = sourceFolderClient;
            _credential = credential;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
            _exportQueue = new KustoExportQueue(
                _kustoClient,
                mainParameterization.Configuration.ExportSlotsRatio);
            _mainParameterization = mainParameterization;
        }

        public static async Task<ClusterExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            KustoClient kustoClient,
            TempFolderService tempFolderService,
            MainParameterization mainParameterization)
        {
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");

            await ValueTask.CompletedTask;

            return new ClusterExportPipeline(
                sourceFolderClient,
                credential,
                kustoClient,
                tempFolderService,
                mainParameterization);
        }

        public async Task RunAsync()
        {
            Trace.WriteLine("Synchronizing source cluster...");
            await SyncDbListAsync();
            await Task.WhenAll(_dbExportPipelineMap.Values.Select(d => d.RunAsync()));
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
            var configMap = _mainParameterization.Source!.DatabaseOverrides.ToImmutableDictionary(
                o => o.Name);

            foreach (var db in newDbNames)
            {
                var dbConfig = _mainParameterization.DatabaseDefault.Override(
                    configMap.ContainsKey(db)
                    ? configMap[db]
                    : new DatabaseOverrideParameterization { Name = db });
                var dbPipeline = await DbExportPipeline.CreateAsync(
                    dbConfig,
                    _sourceFolderClient.GetSubDirectoryClient(db),
                    _credential,
                    _kustoClient,
                    _tempFolderService,
                    _exportQueue);

                _dbExportPipelineMap.Add(dbPipeline.DbName, dbPipeline);
            }

            foreach (var db in obsoleteDbNames)
            {
                _dbExportPipelineMap.Remove(db);
            }
        }
    }
}