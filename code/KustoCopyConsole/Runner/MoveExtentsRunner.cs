using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
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
            if (blockItem.State < BlockState.Ingested)
            {
                throw new InvalidOperationException(
                    $"We shouldn't be in state '{blockItem.State}' at this point");
            }
            if (blockItem.State == BlockState.Ingested)
            {
                blockItem = await MoveExtentsAsync(blockItem, ct);
            }

            return blockItem;
        }

        private async Task<BlockRowItem> MoveExtentsAsync(
            BlockRowItem blockItem,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}