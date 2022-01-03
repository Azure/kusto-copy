using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Export;
using KustoCopyBookmarks.Parameters;
using System.Collections.Immutable;
using System.Diagnostics;

namespace KustoCopyServices
{
    internal class DbExportPlanPipeline
    {
        #region Inner Types
        private class ExportPlan
        {
            public DateTime IngestionTime { get; set; } = DateTime.MinValue;

            public Guid ExtentId { get; set; } = Guid.Empty;

            public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
        }

        private class IngestionSchedule
        {
            public DateTime IngestionTime { get; set; } = DateTime.MinValue;

            public Guid ExtentId { get; set; } = Guid.Empty;
        }

        private class ExtentConfiguration
        {
            public Guid ExtentId { get; set; } = Guid.Empty;

            public DateTime MinCreatedOn { get; set; } = DateTime.MinValue;
        }

        private class ExportResult
        {
            public string Path { get; set; } = string.Empty;

            public long NumRecords { get; set; } = 0;
        }
        #endregion

        private readonly DbExportBookmark _dbExportBookmark;
        private readonly KustoQueuedClient _kustoClient;

        public DbExportPlanPipeline(
            string dbName,
            DbExportBookmark dbExportBookmark,
            KustoQueuedClient kustoClient)
        {
            DbName = dbName;
            _dbExportBookmark = dbExportBookmark;
            _kustoClient = kustoClient;
        }

        public string DbName { get; }

        public async Task RunAsync()
        {
            await ValueTask.CompletedTask;
            //var backfillTask = CopyAsync(true);
            //var currentTask = OrchestrateForwardCopyAsync();

            //await Task.WhenAll(backfillTask, currentTask);
        }
    }
}