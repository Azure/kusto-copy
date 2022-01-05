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
            var backfillTask = PlanAsync(true);
            //var currentTask = OrchestrateForwardCopyAsync();

            //await Task.WhenAll(backfillTask, currentTask);
            await backfillTask;
        }

        private async Task PlanAsync(bool isBackfill)
        {
            var dbEpoch = await GetOrCreateDbEpochAsync(isBackfill);
            var iterations = _dbExportBookmark.GetIterations(dbEpoch.EndCursor);

            while (!dbEpoch.AllIterationsPlanned)
            {
            }
        }

        private async Task<DbEpochData> GetOrCreateDbEpochAsync(bool isBackfill)
        {
            var dbEpoch = _dbExportBookmark.GetDbEpoch(isBackfill);

            if (dbEpoch == null)
            {   //  Create epoch
                var epochInfos = await _kustoClient.ExecuteQueryAsync(
                    KustoPriority.WildcardPriority,
                    DbName,
                    "print CurrentTime=now(), Cursor=cursor_current()",
                    r => new
                    {
                        CurrentTime = (DateTime)r["CurrentTime"],
                        Cursor = (string)r["Cursor"]
                    });
                var epochInfo = epochInfos.First();

                dbEpoch = await _dbExportBookmark.CreateNewEpochAsync(
                    isBackfill,
                    epochInfo.CurrentTime,
                    epochInfo.Cursor);
            }

            return dbEpoch;
        }
    }
}