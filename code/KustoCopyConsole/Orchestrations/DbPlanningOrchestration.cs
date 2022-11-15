﻿using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;

namespace KustoCopyConsole.Orchestrations
{
    public class DbPlanningOrchestration
    {
        #region Inner Types
        private record TableTimeWindowCounts(
            string TableName,
            IImmutableList<TimeWindowCount> TimeWindowCounts);
        #endregion

        private const long TABLE_SIZE_CAP = 1000000000;

        private readonly bool _isContinuousRun;
        private readonly SourceDatabaseParameterization _dbParameterization;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructor
        public static async Task PlanAsync(
            bool isContinuousRun,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbPlanningOrchestration(
                isContinuousRun,
                dbParameterization,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbPlanningOrchestration(
            bool isContinuousRun,
            SourceDatabaseParameterization dbParameterization,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
            _isContinuousRun = isContinuousRun;
            _dbParameterization = dbParameterization;
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            do
            {
                var currentIterationId = await ComputeUnfinishedIterationAsync(ct);
                var cursorWindow = GetLatestCursorWindow();
                var tableNames = await ComputeTableNamesAsync();
                var timeWindowsTasks = tableNames.Select(t => new
                {
                    TableName = t,
                    Task = TableTimeWindowOrchestration.ComputeWindowsAsync(
                        new KustoPriority(currentIterationId, 1, _dbStatus.DbName, t),
                        cursorWindow,
                        null,
                        TABLE_SIZE_CAP,
                        _sourceQueuedClient,
                        ct)
                }).ToImmutableArray();
                //var planningTasks = tableNames
                //    .Select(t => TablePlanningOrchestration.PlanAsync(
                //        currentIteration.IterationId,
                //        t,
                //        _dbStatus,
                //        _sourceQueuedClient,
                //        ct));

                await Task.WhenAll(timeWindowsTasks.Select(t => t.Task));

                var timeWindowsPerTable = timeWindowsTasks
                    .Select(t => new TableTimeWindowCounts(
                        t.TableName,
                        t.Task.Result))
                    .ToImmutableArray();
                var subIteration = await CreateSubIterationAsync(
                    currentIterationId,
                    1,
                    timeWindowsPerTable,
                    ct);
            }
            while (_isContinuousRun);
        }

        private async Task<StatusItem> CreateSubIterationAsync(
            long iterationId,
            long subIterationId,
            IImmutableList<TableTimeWindowCounts> tableTimeWindowCounts,
            CancellationToken ct)
        {
            var groupedByStartTime = tableTimeWindowCounts
                .SelectMany(t => t.TimeWindowCounts.Select(w => new
                {
                    TableName = t.TableName,
                    TimeWindowCount = w
                }))
                .GroupBy(i => i.TimeWindowCount.TimeWindow.StartTime)
                .Select(g => new
                {
                    StartTime = g.Key,
                    TotalCardinality = g.Sum(i => i.TimeWindowCount.Cardinality),
                    Items = g
                });
            var orderedGroupedByStartTime = iterationId == 1
                ? groupedByStartTime.OrderByDescending(g => g.StartTime)
                : groupedByStartTime.OrderBy(g => g.StartTime);
            var groupsToScan = orderedGroupedByStartTime.ToImmutableArray();
            var groupCountToKeep = 0;
            long totalCardinality = 0;

            for (int i = 0; i < groupsToScan.Length; i++)
            {
                var group = groupsToScan[i];

                if (i == 0
                    || totalCardinality + group.TotalCardinality > TABLE_SIZE_CAP)
                {
                    ++groupCountToKeep;
                    totalCardinality += group.TotalCardinality;
                }
                else
                {   //  We don't want the sub iteration to exceed # of rows
                    break;
                }
            }

            var groupsToRetain = groupsToScan.Take(groupCountToKeep);
            var subIteration = StatusItem.CreateSubIteration(
                iterationId,
                subIterationId,
                groupsToRetain
                .Min(g => g.StartTime),
                groupsToRetain
                .SelectMany(g => g.Items)
                .Max(i => i.TimeWindowCount.TimeWindow.EndTime),
                DateTime.UtcNow.Ticks.ToString("x8"));

            await _dbStatus.PersistNewItemsAsync(new[] { subIteration }, ct);

            return subIteration;
        }

        private CursorWindow GetLatestCursorWindow()
        {
            var iterations = _dbStatus.GetIterations();

            if (!iterations.Any())
            {
                throw new InvalidOperationException("There should be an iteration available");
            }
            else
            {
                if (iterations.Count() == 1)
                {
                    return new CursorWindow(null, iterations.First().EndCursor);
                }
                else
                {
                    var lastIteration = iterations.Last();
                    var previousIteration = iterations.Take(iterations.Count() - 1).Last();

                    return new CursorWindow(previousIteration.EndCursor, lastIteration.EndCursor);
                }
            }
        }

        private async Task<long> ComputeUnfinishedIterationAsync(CancellationToken ct)
        {
            var iterations = _dbStatus.GetIterations();

            if (!iterations.Any() || iterations.Last().State == StatusItemState.Done)
            {
                var newIterationId = iterations.Any()
                    ? iterations.Last().IterationId + 1
                    : 1;
                var infoTask = _sourceQueuedClient.ExecuteQueryAsync(
                    new KustoPriority(),
                    _dbStatus.DbName,
                    "print Cursor=cursor_current(), Time=now()",
                    r => new
                    {
                        Cursor = (string)r["Cursor"],
                        Time = (DateTime)r["Time"]
                    });
                var info = (await infoTask).First();
                var newIteration = StatusItem.CreateIteration(
                    newIterationId,
                    info.Cursor,
                    info.Time);

                await _dbStatus.PersistNewItemsAsync(new[] { newIteration }, ct);

                return newIteration.IterationId;
            }
            else
            {
                return iterations.Last().IterationId;
            }
        }

        private async Task<IImmutableList<string>> ComputeTableNamesAsync()
        {
            var existingTableNames = await _sourceQueuedClient.ExecuteQueryAsync(
                new KustoPriority(),
                _dbStatus.DbName,
                ".show tables | project TableName",
                r => (string)r[0]);

            if (!_dbParameterization.TablesToInclude.Any()
                && !_dbParameterization.TablesToExclude.Any())
            {
                return existingTableNames;
            }
            else if (_dbParameterization.TablesToInclude.Any()
                && !_dbParameterization.TablesToExclude.Any())
            {
                var intersectionTableNames = _dbParameterization.TablesToInclude
                    .Intersect(existingTableNames)
                    .ToImmutableArray();

                return intersectionTableNames;
            }
            else
            {

                var exclusionTableNames = existingTableNames
                    .Except(_dbParameterization.TablesToExclude)
                    .ToImmutableArray();

                return exclusionTableNames;
            }
        }
    }
}