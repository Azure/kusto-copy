using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyFoundation.KustoQuery;
using KustoCopySpecific.Bookmarks.ExportPlan;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Intrinsics.Arm;

namespace KustoCopySpecific.Pipelines
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
        #endregion

        private const double TOLERANCE_RATIO_MAX_ROW = 0.15;

        private readonly DbExportPlanBookmark _dbExportPlanBookmark;
        private readonly KustoQueuedClient _kustoClient;
        private readonly long _maxRowsPerTablePerIteration;

        public DbExportPlanPipeline(
            string dbName,
            DbExportPlanBookmark dbExportPlanBookmark,
            KustoQueuedClient kustoClient,
            long maxRowsPerTablePerIteration)
        {
            DbName = dbName;
            _dbExportPlanBookmark = dbExportPlanBookmark;
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
            var lastDbIteration =
                _dbExportPlanBookmark.GetDbIterations(dbEpoch.EndCursor).LastOrDefault();
            var tableNames = await FetchTableNamesAsync(isBackfill, dbEpoch.EpochStartTime);

            while (!dbEpoch.AllIterationsPlanned)
            {
                lastDbIteration =
                    await PlanDbIterationAsync(dbEpoch, lastDbIteration, tableNames);
            }
        }

        private async Task<IImmutableList<string>> FetchTableNamesAsync(
            bool isBackfill,
            DateTime epochStartTime)
        {
            var tableNames = await _kustoClient.ExecuteCommandAsync(
                KustoPriority.QueryPriority(epochStartTime),
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
            var newIteration = lastDbIteration != null ? lastDbIteration.Iteration + 1 : 0;
            var watch = new Stopwatch();
            var iterationMessage =
                $"db '{DbName}' iteration {newIteration} of epoch {dbEpoch.EpochStartTime}";

            Trace.WriteLine($"Start planning {iterationMessage}");
            watch.Start();

            var tableIntervalTasks = tableNames
                .Select(t => FetchTableIntervalAsync(dbEpoch, lastDbIteration, t))
                .ToImmutableList();

            await Task.WhenAll(tableIntervalTasks);
            Trace.TraceInformation($"Computed interval for {iterationMessage}");

            var tableIntervals = tableIntervalTasks
                .Select(t => t.Result)
                .ToImmutableArray();
            var dbIteration = new DbIterationData
            {
                EpochEndCursor = dbEpoch.EndCursor,
                Iteration = newIteration,
                MinIngestionTime = tableIntervals.Select(i => i.MinIngestionTime).Min(),
                MaxIngestionTime = tableIntervals.Select(i => i.MaxIngestionTime).Max()
            };
            var tableExportPlanTasks = tableNames
                .Select(t => ConstructExportPlanAsync(dbEpoch, dbIteration, t))
                .ToImmutableList();

            await Task.WhenAll(tableExportPlanTasks);

            var tableExportPlans = tableExportPlanTasks
                .Select(t => t.Result)
                .ToImmutableArray();

            await _dbExportPlanBookmark.CreateNewDbIterationAsync(dbEpoch, dbIteration, tableExportPlans);

            Trace.WriteLine($"Planning for {iterationMessage} done:  {watch.Elapsed}");

            return dbIteration;
        }

        private async Task<TableExportPlanData> ConstructExportPlanAsync(
            DbEpochData dbEpoch,
            DbIterationData dbIteration,
            string tableName)
        {
            var priority = KustoPriority.QueryPriority(dbIteration.MinIngestionTime
                ?? (dbIteration.MaxIngestionTime ?? dbEpoch.EpochStartTime));
            var ingestionTimes =
                await FetchIngestionSchedulesAsync(dbEpoch, dbIteration, priority, tableName);

            if (ingestionTimes.Any())
            {
                var extentIds = ingestionTimes.Select(i => i.ExtentId).Distinct().ToImmutableArray();
                var extents = await FetchExtentsAsync(tableName, extentIds, priority);

                if (extentIds.Count() != extentIds.Count())
                {   //  Possible if extents got dropped or merged between query & show-command
                    //  Fix is simply to re-fetch
                    return await ConstructExportPlanAsync(
                        dbEpoch,
                        dbIteration,
                        tableName);
                }
                else
                {
                    var extentMap = extents.ToDictionary(e => e.ExtentId, e => e.MinCreatedOn);
                    var steps = ingestionTimes
                        .GroupBy(i => i.ExtentId)
                        .Select(g => new TableExportStepData
                        {
                            IngestionTimes = g.Select(i => i.IngestionTime).ToImmutableArray(),
                            OverrideIngestionTime = extentMap[g.Key]
                        })
                        .ToImmutableArray();
                    var plan = new TableExportPlanData
                    {
                        EpochEndCursor = dbIteration.EpochEndCursor,
                        TableName = tableName,
                        Steps = steps
                    };

                    return plan;
                }
            }
            else
            {   //  Empty iteration for this table
                return new TableExportPlanData
                {
                    EpochEndCursor = dbIteration.EpochEndCursor,
                    TableName = tableName
                };
            }
        }

        private async Task<IImmutableList<ExtentConfiguration>> FetchExtentsAsync(
            string tableName,
            IEnumerable<Guid> extentIds,
            KustoPriority priority)
        {
            var extentIdList = string.Join(", ", extentIds);
            var commandText = @$"
.show table ['{tableName}'] extents ({extentIdList})
| project ExtentId, MinCreatedOn
";
            var extents = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .ExecuteCommandAsync(
                priority,
                DbName,
                commandText,
                r => new ExtentConfiguration
                {
                    ExtentId = (Guid)r["ExtentId"],
                    MinCreatedOn = (DateTime)r["MinCreatedOn"]
                });

            return extents;
        }

        private async Task<ImmutableArray<IngestionSchedule>> FetchIngestionSchedulesAsync(
            DbEpochData dbEpoch,
            DbIterationData dbIteration,
            KustoPriority priority,
            string tableName)
        {
            var schedules = await _kustoClient
                .SetParameter("TargetTableName", tableName)
                .SetParameter("MinIngestionTime", dbIteration.MinIngestionTime ?? DateTime.MinValue)
                .SetParameter("MaxIngestionTime", dbIteration.MaxIngestionTime ?? DateTime.MaxValue)
                .SetParameter("StartCursor", dbEpoch.StartCursor ?? string.Empty)
                .SetParameter("EndCursor", dbEpoch.EndCursor)
                .ExecuteQueryAsync(
                priority,
                DbName,
                @"
declare query_parameters(TargetTableName: string);
declare query_parameters(MinIngestionTime: datetime);
declare query_parameters(MaxIngestionTime: datetime);
declare query_parameters(StartCursor: string);
declare query_parameters(EndCursor: string);
table(TargetTableName)
| where cursor_before_or_at(EndCursor)
| where cursor_after(StartCursor)
| where ingestion_time() >= MinIngestionTime
| where ingestion_time() <= MaxIngestionTime
| summarize by IngestionTime=ingestion_time(), ExtentId=extent_id()
| order by IngestionTime asc",
                r => new IngestionSchedule
                {
                    IngestionTime = (DateTime)r["IngestionTime"],
                    ExtentId = (Guid)r["ExtentId"]
                });

            return schedules;
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
                KustoPriority.QueryPriority(dbEpoch.EpochStartTime),
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
            var dbEpoch = _dbExportPlanBookmark.GetDbEpoch(isBackfill);

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

                dbEpoch = await _dbExportPlanBookmark.CreateNewEpochAsync(
                    isBackfill,
                    epochInfo.CurrentTime,
                    epochInfo.Cursor);
            }

            return dbEpoch;
        }
    }
}