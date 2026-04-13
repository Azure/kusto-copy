using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class AwaitMovedRunner : AwaitCommandRunner
    {
        public AwaitMovedRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        protected override BlockState InitialState => BlockState.ExtentMoving;

        protected override BlockState ResetState => BlockState.Ingested;

        protected override Uri GetClusterUri(ActivityParameterization activity)
            => activity.GetDestinationTableIdentity().ClusterUri;

        protected override BlockRecord ResetBlock(BlockRecord block)
        {
            return block with { MoveOperationId = string.Empty };
        }

        protected override string GetOperationId(BlockRecord block)
        {
            return block.MoveOperationId;
        }

        protected override async Task ProcessOperationAsync(
            OperationStatus status,
            BlockRecord block,
            DbCommandClient dbClient,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var newBlock = block with
            {
                State = BlockState.ExtentMoved,
                MoveOperationId = string.Empty
            };

            Database.Blocks.UpdateRecord(block, newBlock);
            await Task.CompletedTask;
        }
    }
}