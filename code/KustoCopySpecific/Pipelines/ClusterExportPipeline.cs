using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyFoundation.KustoQuery;
using KustoCopySpecific.Bookmarks.DbExportStorage;
using KustoCopySpecific.Bookmarks.ExportPlan;
using KustoCopySpecific.Bookmarks.IterationExportStorage;
using KustoCopySpecific.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Pipelines
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

        private readonly DataLakeDirectoryClient _rootTempFolderClient;
        private readonly DataLakeDirectoryClient _sourceFolderClient;
        private readonly TokenCredential _credential;
        private readonly KustoQueuedClient _kustoClient;
        private readonly IImmutableDictionary<string, DbPipelines> _dbPipelinesMap;
        private readonly MainParameterization _mainParameterization;

        private ClusterExportPipeline(
            DataLakeDirectoryClient rootTempFolderClient,
            DataLakeDirectoryClient sourceFolderClient,
            TokenCredential credential,
            KustoQueuedClient kustoClient,
            IImmutableDictionary<string, DbPipelines> dbPipelinesMap,
            MainParameterization mainParameterization)
        {
            _rootTempFolderClient = rootTempFolderClient;
            _sourceFolderClient = sourceFolderClient;
            _credential = credential;
            _kustoClient = kustoClient;
            _dbPipelinesMap = dbPipelinesMap;
            _mainParameterization = mainParameterization;
        }

        public static async Task<ClusterExportPipeline> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential credential,
            KustoQueuedClient kustoClient,
            MainParameterization mainParameterization)
        {
            var sourceFolderClient = folderClient.GetSubDirectoryClient("source");
            var rootTempFolderClient = folderClient.GetSubDirectoryClient("temp");
            //  Fetch the database list from the cluster
            var allDbNames = await kustoClient.ExecuteCommandAsync(
                KustoPriority.WildcardPriority,
                string.Empty,
                ".show databases | project DatabaseName",
                r => (string)r["DatabaseName"]);
            var databases = mainParameterization.Source!.Databases
                ?? allDbNames.Select(n => new SourceDatabaseParameterization
                {
                    Name = n
                }).ToImmutableArray();
            var pipelineList = new List<DbPipelines>();

            await rootTempFolderClient.DeleteIfExistsAsync();
            Trace.WriteLine($"Source databases:  {{{string.Join(", ", allDbNames.Sort())}}}");

            var pipelineTasks = databases
                .Select(async db =>
                {
                    var dbConfig = mainParameterization.Source!.DatabaseDefault.Override(
                        db.DatabaseOverrides);
                    var dbFolderClient = sourceFolderClient.GetSubDirectoryClient(db.Name);
                    var planFileClient = dbFolderClient.GetFileClient("plan-db.bookmark");
                    var dbExportPlanBookmark = await DbExportPlanBookmark.RetrieveAsync(
                        planFileClient,
                        credential);
                    var storageFileClient = dbFolderClient.GetFileClient("db-storage.bookmark");
                    var dbStorageBookmark = await DbExportStorageBookmark.RetrieveAsync(
                        storageFileClient,
                        credential);
                    var iterationFederation = new DbIterationStorageFederation(
                        dbStorageBookmark,
                        dbFolderClient,
                        credential);
                    var dbExportPlan = new DbExportPlanPipeline(
                        db.Name!,
                        dbExportPlanBookmark,
                        kustoClient,
                        dbConfig.MaxRowsPerTablePerIteration!.Value);
                    var dbExportExecution = new DbExportExecutionPipeline(
                        rootTempFolderClient,
                        db.Name!,
                        dbExportPlanBookmark,
                        iterationFederation,
                        kustoClient,
                        mainParameterization.Source!.ConcurrentExportCommandCount);
                    var pipelines = new DbPipelines(dbExportPlan, dbExportExecution);

                    return pipelines;
                })
                .ToImmutableArray();

            await Task.WhenAll(pipelineTasks);

            var dbPipelinesMap = pipelineTasks
                .Select(t => t.Result)
                .ToImmutableDictionary(p => p.DbExportPlan.DbName);

            return new ClusterExportPipeline(
                rootTempFolderClient,
                sourceFolderClient,
                credential,
                kustoClient,
                dbPipelinesMap,
                mainParameterization);
        }

        public IEnumerable<DbExportPlanBookmark> ExportExecutionPipelines =>
            _dbPipelinesMap.Values.Select(p => p.DbExportExecution.DbExportPlanBookmark);

        public async Task RunAsync()
        {
            var planTasks = _dbPipelinesMap.Values.Select(d => d.DbExportPlan.RunAsync());
            var executionTasks =
                _dbPipelinesMap.Values.Select(d => d.DbExportExecution.RunAsync());

            await Task.WhenAll(planTasks.Concat(executionTasks));
        }
    }
}