using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public class TablePlanningOrchestration
    {
        private readonly string _tableName;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task PlanAsync(
            string tableName,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                tableName,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private TablePlanningOrchestration(
            string tableName,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
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