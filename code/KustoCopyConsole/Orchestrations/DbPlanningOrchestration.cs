using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using KustoCopyConsole.Storage;

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
                var endCursors = await _sourceQueuedClient.ExecuteQueryAsync(
                    new KustoPriority(),
                    _dbStatus.DbName,
                    "print cursor_current()",
                    r => (string)r[0]);
                var endCursor = endCursors.First();
                var newIteration = StatusItem.CreateIteration(
                    newIterationId,
                    endCursor);
                var newSubIteration = StatusItem.CreateSubIteration(
                    newIterationId,
                    endCursor,
                    1,
                    null,
                    null);

                await _dbStatus.PersistNewItemsAsync(new[] { newIteration, newSubIteration }, ct);

                return newIteration;
            }
            else
            {
                return iterations.Last();
            }
        }
    }
}