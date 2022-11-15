using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Orchestrations
{
    public class DbPlanningOrchestration
    {
        #region Inner Types
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
                    .ToImmutableDictionary(t => t.TableName, t => t.Task.Result);
                var subIteration = await CreateSubIterationAsync();
            }
            while (_isContinuousRun);
        }

        private Task<StatusItem> CreateSubIterationAsync()
        {
            throw new NotImplementedException();
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
                var newSubIteration = StatusItem.CreateSubIteration(newIterationId, 1);

                await _dbStatus.PersistNewItemsAsync(
                    new[] { newIteration, newSubIteration },
                    ct);

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