using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib;

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
            IEnumerable<BlockRecord> blocks,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var iterationKey = blocks.First().BlockKey.IterationKey;
            var tableId = activityParam.GetDestinationTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                tableId.ClusterUri,
                tableId.DatabaseName);
            var newBlocks = blocks
                .Select(b => b with
                {
                    State = BlockState.ExtentMoved,
                    MoveOperationId = string.Empty
                });
            
            //  Remove tags on destination tables
            await dbClient.CleanExtentTagsAsync(
                new KustoPriority(iterationKey),
                activityParam.GetDestinationTableIdentity().TableName,
                blocks.Select(b => b.BlockTag),
                ct);
            using (var tx = Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        b => b.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();
                Database.Blocks.AppendRecords(newBlocks, tx);

                tx.Complete();
            }
        }
    }
}