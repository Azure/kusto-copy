using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentRunner : RunnerBase
    {
        #region Inner types
        private record BlockExtents(BlockRecord Block, IEnumerable<ExtentRecord> Extents);
        #endregion

        private const int MAXIMUM_EXTENT_MOVING = 100;

        public MoveExtentRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            DbClientFactory dbClientFactory,
            AzureBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(15))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var blockExtentsToMove = GetExtentsToMove();

                if (blockExtentsToMove.Any())
                {
                    await MoveAsync(blockExtentsToMove, ct);
                }
                else
                {
                    await SleepAsync(ct);
                }
            }
        }

        private IEnumerable<BlockExtents> GetExtentsToMove()
        {
            var firstBlock = Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                .OrderBy(b => b.BlockKey.ActivityName)
                .ThenBy(b => b.BlockKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (firstBlock == null)
            {
                return Array.Empty<BlockExtents>();
            }
            else
            {
                var blocks = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                    .Where(pf => pf.Equal(b => b.BlockKey.ActivityName, firstBlock.BlockKey.ActivityName))
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationId, firstBlock.BlockKey.IterationId))
                    .OrderBy(b => b.BlockKey.BlockId)
                    .Take(MAXIMUM_EXTENT_MOVING)
                    .ToImmutableArray();
                var blockExtents = new List<BlockExtents>(MAXIMUM_EXTENT_MOVING);
                var totalExtentCount = 0;

                foreach (var block in blocks)
                {
                    var extents = Database.Extents.Query()
                        .Where(pf => pf.Equal(e => e.BlockKey.ActivityName, firstBlock.BlockKey.ActivityName))
                        .Where(pf => pf.Equal(e => e.BlockKey.IterationId, firstBlock.BlockKey.IterationId))
                        .Where(pf => pf.Equal(e => e.BlockKey.BlockId, block.BlockKey.BlockId))
                        .ToImmutableArray();

                    if (totalExtentCount == 0
                        || totalExtentCount + extents.Length <= MAXIMUM_EXTENT_MOVING)
                    {
                        blockExtents.Add(new BlockExtents(block, extents));
                        totalExtentCount += extents.Length;
                    }
                    else
                    {
                        return blockExtents;
                    }
                }

                return blockExtents;
            }
        }

        private async Task MoveAsync(
            IEnumerable<BlockExtents> blockExtentsToMove,
            CancellationToken ct)
        {
            var iterationKey = blockExtentsToMove.First().Block.BlockKey.ToIterationKey();
            var tempTableName = GetTempTable(iterationKey).TempTableName;
            var destinationTable = Parameterization.Activities[iterationKey.ActivityName]
                .GetDestinationTableIdentity();
            var commandClient = DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var extentCount = await commandClient.MoveExtentsAsync(
                new KustoPriority(iterationKey),
                tempTableName,
                destinationTable.TableName,
                blockExtentsToMove
                .Select(be => be.Extents)
                .SelectMany(m => m)
                .Select(e => e.ExtentId),
                ct);
            var cleanCount = await commandClient.CleanExtentTagsAsync(
                new KustoPriority(iterationKey),
                destinationTable.TableName,
                blockExtentsToMove.Select(be => be.Block.BlockTag),
                ct);

            CommitMove(iterationKey, blockExtentsToMove);
        }

        private void CommitMove(
            IterationKey iterationKey,
            IEnumerable<BlockExtents> blockExtentsToMove)
        {
            using (var tx = Database.Database.CreateTransaction())
            {
                Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(u => u.BlockKey.IterationId, iterationKey.IterationId))
                    .Where(pf => pf.In(
                        u => u.BlockKey.BlockId,
                        blockExtentsToMove.Select(be => be.Block.BlockKey.BlockId)))
                    .Delete();
                Database.Extents.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(u => u.BlockKey.IterationId, iterationKey.IterationId))
                    .Where(pf => pf.In(
                        u => u.BlockKey.BlockId,
                        blockExtentsToMove.Select(be => be.Block.BlockKey.BlockId)))
                    .Delete();
                Database.BlobUrls.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey.ActivityName, iterationKey.ActivityName))
                    .Where(pf => pf.Equal(u => u.BlockKey.IterationId, iterationKey.IterationId))
                    .Where(pf => pf.In(
                        u => u.BlockKey.BlockId,
                        blockExtentsToMove.Select(be => be.Block.BlockKey.BlockId)))
                    .Delete();

                Database.Blocks.AppendRecords(
                    blockExtentsToMove
                    .Select(be => be.Block with
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