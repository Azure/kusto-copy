using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestration
{
    /// <summary>
    /// This orchestration is responsible for table's planning.
    /// </summary>
    internal class SourceTablePlanningOrchestration : SubOrchestrationBase
    {
        public SourceTablePlanningOrchestration(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
            : base(rowItemGateway, dbClientFactory, parameterization)
        {
        }

        protected override async Task OnStartProcessAsync(CancellationToken ct)
        {
            var planningTableIterations = RowItemGateway.InMemoryCache
                .SourceTableMap
                .Values
                .Select(t => t.IterationMap.Values)
                .SelectMany(i => i)
                .Select(i => i.RowItem)
                .Where(i => i.ParseState<SourceTableState>() == SourceTableState.Planning);

            foreach (var i in planningTableIterations)
            {
                BackgroundTaskContainer.AddTask(OnPlanningIterationAsync(i, ct));
            }

            await Task.CompletedTask;
        }

        protected override void OnProcessRowItemAppended(RowItemAppend e, CancellationToken ct)
        {
            base.OnProcessRowItemAppended(e, ct);

            if (e.Item.RowType == RowType.SourceTable
                && e.Item.ParseState<SourceTableState>() == SourceTableState.Planning)
            {
                BackgroundTaskContainer.AddTask(OnPlanningIterationAsync(e.Item, ct));
            }
        }

        private Task OnPlanningIterationAsync(RowItem item, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}