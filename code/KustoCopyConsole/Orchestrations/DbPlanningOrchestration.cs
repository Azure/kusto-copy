using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class DbPlanningOrchestration
    {
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
                var currentIteration = await ComputeCurrentIterationAsync(ct);
                //var tableNamesTask = _sourceQueuedClient.ExecuteQueryAsync(
                //    new KustoPriority(),
                //    _dbStatus.DbName,
                //    ".show tables | project TableName",
                //    r => (string)r[0]);
                //var tableNames = ComputeTableNames(await tableNamesTask);
            }
            while (_isContinuousRun);
        }

        private async Task<StatusItem> ComputeCurrentIterationAsync(CancellationToken ct)
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

                return newIteration;
            }
            else
            {
                return iterations.Last();
            }
        }

        private IImmutableList<string> ComputeTableNames(
            IImmutableList<string> existingTableNames)
        {
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