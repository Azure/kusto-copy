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
        private class TableInterval
        {
            public string TableName { get; set; } = string.Empty;

            public DateTime? MinIngestionTime { get; set; } = null;

            public DateTime? MaxIngestionTime { get; set; } = null;
        }
        #endregion

        private const double TOLERANCE_RATIO_MAX_ROW = 0.15;

        private readonly DbExportBookmark _dbExportBookmark;
        private readonly KustoQueuedClient _kustoClient;
        private readonly long _maxRowsPerTablePerIteration;

        public DbExportPlanPipeline(
            string dbName,
            DbExportBookmark dbExportBookmark,
            KustoQueuedClient kustoClient,
            long maxRowsPerTablePerIteration)
        {
            DbName = dbName;
            _dbExportBookmark = dbExportBookmark;
            _kustoClient = kustoClient;
            _maxRowsPerTablePerIteration = maxRowsPerTablePerIteration;
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
            var dbIterations = _dbExportBookmark.GetDbIterations(dbEpoch.EndCursor);
            var tableNames = await FetchTableNamesAsync(isBackfill, dbEpoch.EpochStartTime);

            while (!dbEpoch.AllIterationsPlanned)
            {
                await PlanDbIterationAsync(dbEpoch, dbIterations.LastOrDefault(), tableNames);
            }
        }

        private async Task<IImmutableList<string>> FetchTableNamesAsync(
            bool isBackfill,
            DateTime epochStartTime)
        {
            var tableNames = await _kustoClient.ExecuteCommandAsync(
                new KustoPriority(KustoOperation.QueryOrCommand, isBackfill, epochStartTime),
                DbName,
                ".show tables | project TableName",
                r => (string)r["TableName"]);

            return tableNames;
        }

        private async Task<DbIterationData?> PlanDbIterationAsync(
            DbEpochData dbEpoch,
            DbIterationData? lastDbIteration,
            IImmutableList<string> tableNames)
        {
            var tableIntervalTasks = tableNames
                .Select(t => FetchTableIntervalAsync(dbEpoch, lastDbIteration, t))
                .ToImmutableList();

            await Task.WhenAll(tableIntervalTasks);

            var tableIntervals = tableIntervalTasks
                .Select(t => t.Result)
                .ToImmutableArray();
            var dbIteration = new DbIterationData
            {
                EpochEndCursor = dbEpoch.EndCursor,
                Iteration = lastDbIteration != null ? lastDbIteration.Iteration + 1 : 0,
                MinIngestionTime = tableIntervals.Select(i => i.MinIngestionTime).Min(),
                MaxIngestionTime = tableIntervals.Select(i => i.MaxIngestionTime).Max()
            };

            throw new NotImplementedException();
        }

        private async Task<TableInterval> FetchTableIntervalAsync(
            DbEpochData dbEpoch,
            DbIterationData? lastDbIteration,
            string tableName)
        {
            var naiveIntervals = await _kustoClient
                .SetParameter("TargetTable", tableName)
                .SetParameter("StartCursor", dbEpoch.StartCursor ?? string.Empty)
                .SetParameter("EndCursor", dbEpoch.EndCursor)
                .ExecuteQueryAsync(
                KustoPriority.WildcardPriority,
                DbName,
                @"
declare query_parameters(TargetTable: string);
declare query_parameters(StartCursor: string);
declare query_parameters(EndCursor: string);
table(TargetTable)
| where cursor_after(StartCursor)
| where cursor_before_or_at(EndCursor)
| summarize Cardinality=count(), Min=min(ingestion_time()), Max=max(ingestion_time())
",
                r => new
                {
                    Cardinality = (long)r["Cardinality"],
                    Min = r["Min"].To<DateTime>(),
                    Max = r["Max"].To<DateTime>()
                });
            var naiveInterval = naiveIntervals.First();

            if (naiveInterval.Cardinality
                <= _maxRowsPerTablePerIteration * (1 + TOLERANCE_RATIO_MAX_ROW))
            {   //  No Min/Max as we do not need to narrow the interval
                return new TableInterval { TableName = tableName };
            }
            else
            {
                throw new NotImplementedException();
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