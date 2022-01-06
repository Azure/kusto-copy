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
        #region MyRegion
        private class DbPipelines
        {
            public DbPipelines(
                DbExportPlanPipeline dbExportPlan,
                DbExportExecutionPipeline dbExportExecution)
            {
                DbExportPlan = dbExportPlan;
                DbExportExecution = dbExportExecution;
            }

            public DbExportPlanPipeline DbExportPlan { get; }

            public DbExportExecutionPipeline DbExportExecution { get; }
        }
        #endregion

        private readonly DataLakeDirectoryClient _sourceFolderClient;
        private readonly TokenCredential _credential;
        private readonly KustoQueuedClient _kustoClient;
        private readonly TempFolderService _tempFolderService;
        private readonly IDictionary<string, DbPipelines> _dbPipelinesMap =
            new Dictionary<string, DbPipelines>();
        private readonly MainParameterization _mainParameterization;

        private ClusterExportPipeline(
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoQueuedClient kustoClient,
            TempFolderService tempFolderService,
            MainParameterization mainParameterization)
        {
            _sourceFolderClient = sourceFolderClient;
            _credential = credential;
            _kustoClient = kustoClient;
            _tempFolderService = tempFolderService;
            _mainParameterization = mainParameterization;
        }

        public static async Task<ClusterExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            KustoQueuedClient kustoClient,
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
            await SyncDbListAsync();

            var planTasks = _dbPipelinesMap.Values.Select(d => d.DbExportPlan.RunAsync());
            var executionTasks =
                _dbPipelinesMap.Values.Select(d => d.DbExportExecution.RunAsync());

            await Task.WhenAll(planTasks.Concat(executionTasks));
        }

        private async Task SyncDbListAsync()
        {
            //  Fetch the database list from the cluster
            var nextDbNames = await _kustoClient.ExecuteCommandAsync(
                KustoPriority.WildcardPriority,
                string.Empty,
                ".show databases | project DatabaseName",
                r => (string)r["DatabaseName"]);
            var currentDbNames = _dbPipelinesMap.Keys.ToImmutableArray();
            var obsoleteDbNames = currentDbNames.Except(nextDbNames);
            var newDbNames = nextDbNames.Except(currentDbNames);
            var configMap = _mainParameterization.Source!.DatabaseOverrides.ToImmutableDictionary(
                o => o.Name);

            if (newDbNames.Concat(obsoleteDbNames).Any())
            {
                Trace.WriteLine("Synchronizing source cluster...");
            }
            foreach (var db in newDbNames)
            {
                var dbConfig = _mainParameterization.DatabaseDefault.Override(
                    configMap.ContainsKey(db)
                    ? configMap[db]
                    : new DatabaseOverrideParameterization { Name = db });
                var sourceFileClient = _sourceFolderClient
                    .GetSubDirectoryClient(db)
                    .GetFileClient("plan-db.bookmark");
                var dbExportPlanBookmark = await DbExportPlanBookmark.RetrieveAsync(
                    sourceFileClient,
                    _credential);
                var dbExportPlan = new DbExportPlanPipeline(
                    db,
                    dbExportPlanBookmark,
                    _kustoClient,
                    dbConfig.MaxRowsPerTablePerIteration!.Value);
                var dbExportExecution = new DbExportExecutionPipeline(
                    db,
                    dbExportPlanBookmark,
                    _kustoClient,
                    _mainParameterization.Configuration.ExportSlotsRatio / 100.0);
                var pipelines = new DbPipelines(dbExportPlan, dbExportExecution);

                _dbPipelinesMap.Add(dbExportPlan.DbName, pipelines);
            }

            foreach (var db in obsoleteDbNames)
            {
                _dbPipelinesMap.Remove(db);
            }
        }
    }
}