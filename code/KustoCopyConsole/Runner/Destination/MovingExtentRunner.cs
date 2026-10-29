using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner.Destination
{
    internal class MovingExtentRunner : StartAwaitCommandRunnerBase
    {
        public MovingExtentRunner(RunnerParameters parameters)
           : base(parameters, false, TimeSpan.FromSeconds(15))
        {
        }

        public async override Task RunAsync(CancellationToken ct)
        {
            if (ShouldIngestionRun)
            {
                await base.RunAsync(ct);
            }
        }

        protected override int? MaxCapacity => null;

        protected override BlockState InitialState => BlockState.Ingested;

        protected override BlockState TransitionState => BlockState.ExtentMoving;

        protected override void ResetLostBlock(BlockRecord lostBlock)
        {
            Database.Blocks.UpdateRecord(
                lostBlock,
                lostBlock with
                {
                    State = BlockState.Ingested,
                    MoveOperationId = string.Empty
                });
        }

        protected override void RetryFailedBlock(BlockRecord failedBlock, string status)
        {
            ResetLostBlock(failedBlock);
        }

        protected override async Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct)
        {
            var dbCommandClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var capacity = await dbCommandClient.ShowClusterNodeCountAsync(
                KustoPriority.HighestPriority,
                ct);

            return capacity;
        }

        protected override async Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct)
        {
            var destinationTable = activityParam.GetDestinationTableIdentity();
            var dbClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var tempTableName = GetTempTable(blockRecord.BlockKey.IterationKey).TempTableName;
            var extentIds = Database.Extents.Query()
                .Where(pf => pf.Equal(e => e.BlockKey, blockRecord.BlockKey))
                .Select(e => e.ExtentId)
                .ToArray();
            var operationId = await dbClient.MoveExtentsAsync(
                new KustoPriority(blockRecord.BlockKey),
                tempTableName,
                destinationTable.TableName,
                extentIds,
                ct);
            var newBlockRecord = blockRecord with
            {
                MoveOperationId = operationId
            };

            return newBlockRecord;
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