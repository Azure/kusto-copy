using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentsRunner : RunnerBase
    {
        public MoveExtentsRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory)
        {
        }

        public async Task<BlockRowItem> RunAsync(BlockRowItem blockItem, CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}