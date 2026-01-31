using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using TrackDb.Lib.Predicate;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentRunner : RunnerBase
    {
        private const int MAXIMUM_EXTENT_MOVING = 100;

        public MoveExtentRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var extentsToMove = await GetExtentsToMoveAsync(ct);

                if (extentsToMove != null)
                {
                    await MoveAsync(extentsToMove, ct);
                }
                else
                {
                    await SleepAsync(ct);
                }
            }
        }

        private async Task<IEnumerable<ExtentRecord>> GetExtentsToMoveAsync(CancellationToken ct)
        {
            using (var tx = Database.CreateTransaction())
            {
                var readyToMoveBlockIdByIterationKeys = Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                    //  At most that number
                    .Take(MAXIMUM_EXTENT_MOVING)
                    .AsEnumerable()
                    .GroupBy(b => b.BlockKey.IterationKey, b => b.BlockKey.BlockId)
                    .ToImmutableArray();
                var extents = new List<ExtentRecord>(MAXIMUM_EXTENT_MOVING);

                foreach (var iterationGroup in readyToMoveBlockIdByIterationKeys)
                {
                    var readyToMoveExtentsByBlockId = Database.Extents.Query(tx)
                        .Where(pf => pf.In(e => e.BlockKey.BlockId, iterationGroup))
                        .GroupBy(e => e.BlockKey.BlockId);

                    foreach (var blockGroup in readyToMoveExtentsByBlockId)
                    {
                        if (extents.Count + blockGroup.Count() <= MAXIMUM_EXTENT_MOVING)
                        {
                            extents.AddRange(blockGroup);
                        }
                    }
                }

                //  Ensure the ingested blocks are committed to logs
                //  We need to ensure that before we move the extent and we can't
                //  detect ingested-state anymore
                await tx.CompleteAsync(ct);

                return extents;
            }
        }

        private async Task MoveAsync(IEnumerable<ExtentRecord> extents, CancellationToken ct)
        {
            var extentsByIteration = extents
                .GroupBy(e => e.BlockKey.IterationKey);

            foreach (var g in extentsByIteration)
            {
                var iterationKey = g.Key;
                var tempTableName = GetTempTable(iterationKey).TempTableName;
                var destinationTable = Parameterization.Activities[iterationKey.ActivityName]
                    .GetDestinationTableIdentity();
                var commandClient = DbClientFactory.GetDbCommandClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName);
                var extentIds = g.Select(e => e.ExtentId);
                var extentCount = await commandClient.MoveExtentsAsync(
                    new KustoPriority(iterationKey),
                    tempTableName,
                    destinationTable.TableName,
                    extentIds,
                    ct);
                var blockIds = g
                    .Select(e => e.BlockKey.BlockId)
                    .Distinct();
                var blocks = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                    .ToImmutableArray();
                var cleanCount = await commandClient.CleanExtentTagsAsync(
                    new KustoPriority(iterationKey),
                    destinationTable.TableName,
                    blocks.Select(be => be.BlockTag),
                    ct);

                CommitMove(iterationKey, blocks);
            }
        }

        private void CommitMove(IterationKey iterationKey, IEnumerable<BlockRecord> blocks)
        {
            using (var tx = Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(i => i.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        b => b.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();

                Database.Blocks.AppendRecords(
                    blocks
                    .Select(b => b with
                    {
                        State = BlockState.ExtentMoved,
                        BlockTag = string.Empty
                    }),
                    tx);

                tx.Complete();
            }
        }
    }
}