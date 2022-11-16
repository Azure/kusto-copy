using KustoCopyConsole.KustoQuery;
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
                var tableNames = await ComputeTableNamesAsync();
                var subIteration = await ComputeUnfinishedSubIterationAsync(tableNames, ct);
                var planningTasks = tableNames
                    .Select(t => TablePlanningOrchestration.PlanAsync(
                        new KustoPriority(
                            subIteration.IterationId,
                            subIteration.SubIterationId!.Value,
                            _dbStatus.DbName,
                            t),
                        subIteration,
                        GetLatestCursorWindow(subIteration.IterationId),
                        _dbStatus,
                        _sourceQueuedClient,
                        ct)).ToImmutableArray();

                await Task.WhenAll(planningTasks);
            }
            while (_isContinuousRun);
        }

        private async Task<StatusItem> ComputeUnfinishedSubIterationAsync(
            IImmutableList<string> tableNames,
            CancellationToken ct)
        {
            var iterationId = await ComputeUnfinishedIterationAsync(ct);
            var subIterations = _dbStatus
                .GetSubIterations(iterationId)
                .Where(i => i.State == StatusItemState.Initial);

            if (subIterations.Any())
            {
                return subIterations.First();
            }
            else
            {
                var subIterationId = subIterations.Any()
                    ? subIterations.Max(i => i.SubIterationId!) + 1
                    : 1;
                var cursorWindow = GetLatestCursorWindow(iterationId);
                //  Find time window for each table to establish sub iteration
                var timeWindowsTasks = tableNames.Select(t => new
                {
                    TableName = t,
                    Task = TableTimeWindowOrchestration.ComputeWindowsAsync(
                        new KustoPriority(iterationId, subIterationId, _dbStatus.DbName, t),
                        cursorWindow,
                        null,
                        TABLE_SIZE_CAP,
                        _sourceQueuedClient,
                        ct)
                }).ToImmutableArray();

                await Task.WhenAll(timeWindowsTasks.Select(t => t.Task));

                var timeWindowsPerTable = timeWindowsTasks
                    .Select(t => new TableTimeWindowCounts(
                        t.TableName,
                        t.Task.Result))
                    .ToImmutableArray();
                var subIteration = await CreateSubIterationAsync(
                    iterationId,
                    1,
                    timeWindowsPerTable,
                    ct);

                return subIteration;
            }
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

        private CursorWindow GetLatestCursorWindow(long iterationId)
        {
            var iteration = _dbStatus.GetIteration(iterationId);

            if (iterationId == 1)
            {
                return new CursorWindow(null, iteration.EndCursor);
            }
            else
            {
                var previousIteration = _dbStatus.GetIteration(iterationId - 1);

                return new CursorWindow(previousIteration.EndCursor, previousIteration.EndCursor);
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