using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public class DbPlanningOrchestration
    {
        private readonly bool _isContinuousRun;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructor
        public static async Task PlanAsync(
            bool isContinuousRun,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new DbPlanningOrchestration(
                isContinuousRun,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private DbPlanningOrchestration(
            bool isContinuousRun,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
            _isContinuousRun = isContinuousRun;
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            do
            {
                var iterations = _dbStatus.GetIterations();

                if (!iterations.Any() || iterations.Last().State == StatusItemState.Done)
                {
                    var newIterationId = iterations.Any()
                        ? iterations.Last().IterationId + 1
                        : 0;
                    var endCursors = await _sourceQueuedClient.ExecuteQueryAsync(
                        new KustoPriority(),
                        _dbStatus.DbName,
                        "print cursor_current()",
                        r => (string)r[0]);
                    var endCursor = endCursors.First();
                    var newIteration = StatusItem.CreateIteration(
                        newIterationId,
                        endCursor);

                    await _dbStatus.PersistNewItemsAsync(new[] { newIteration }, ct);
                }
            }
            while (_isContinuousRun);
        }
    }
}