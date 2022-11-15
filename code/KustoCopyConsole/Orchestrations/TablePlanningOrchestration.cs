using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public class TablePlanningOrchestration
    {
        private readonly string _tableName;
        private readonly StatusItem _subIteration;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;

        #region Constructors
        public static async Task PlanAsync(
            string tableName,
            StatusItem subIteration,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            CancellationToken ct)
        {
            var orchestration = new TablePlanningOrchestration(
                tableName,
                subIteration,
                dbStatus,
                sourceQueuedClient);

            await orchestration.RunAsync(ct);
        }

        private TablePlanningOrchestration(
            string tableName,
            StatusItem subIteration,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient)
        {
            _tableName = tableName;
            _subIteration = subIteration;
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