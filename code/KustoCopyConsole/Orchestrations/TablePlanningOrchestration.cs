using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public class TablePlanningOrchestration
    {
        private readonly long _iterationId;
        private readonly string _tableName;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task PlanAsync(
            long iterationId,
            string tableName,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                iterationId,
                tableName,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private TablePlanningOrchestration(
            long iterationId,
            string tableName,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
            _iterationId = iterationId;
            _tableName = tableName;
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
        }
        #endregion

        private Task RunAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}