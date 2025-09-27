using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;

namespace KustoCopyConsole.Runner
{
    internal class MoveExtentRunner : RunnerBase
    {
        #region Inner types
        private record MovingBlocks(IterationKey IterationKey, IEnumerable<ExtentRecord> Extents);
        #endregion

        private const int MAXIMUM_EXTENT_MOVING = 100;

        public MoveExtentRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                var blockExtentsToMove = GetExtentsToMove();

                if (blockExtentsToMove != null)
                {
                    await MoveAsync(blockExtentsToMove, ct);
                }
                else
                {
                    await SleepAsync(ct);
                }
            }
        }

        private MovingBlocks? GetExtentsToMove()
        {
            var iterationKey = RunnerParameters.Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                .OrderBy(b => b.BlockKey.IterationKey.ActivityName)
                .ThenBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(1)
                .Select(b => b.BlockKey.IterationKey)
                .FirstOrDefault();

            if (iterationKey == null)
            {
                return null;
            }
            else
            {
                return GetExtentsToMove(iterationKey);
            }
        }

        private MovingBlocks GetExtentsToMove(IterationKey iterationKey)
        {
            var ingestedBlockIds = RunnerParameters.Database.Blocks.Query()
                .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.Equal(b => b.State, BlockState.Ingested))
                .OrderBy(b => b.BlockKey.BlockId)
                .Take(MAXIMUM_EXTENT_MOVING)
                .AsEnumerable()
                .Select(b => b.BlockKey.BlockId)
                .ToImmutableArray();
            var ingestedExtentsByBlockId = RunnerParameters.Database.Extents.Query()
                .Where(pf => pf.Equal(e => e.BlockKey.IterationKey, iterationKey))
                .Where(pf => pf.In(e => e.BlockKey.BlockId, ingestedBlockIds))
                .ToImmutableArray()
                .GroupBy(e => e.BlockKey.BlockId)
                .ToImmutableDictionary(g => g.Key, g => g.ToImmutableList());
            var totalExtentCount = 0;

            foreach (var blockId in ingestedBlockIds)
            {
                var extents = ingestedExtentsByBlockId[blockId];

                if (totalExtentCount == 0
                    || totalExtentCount + extents.Count <= MAXIMUM_EXTENT_MOVING)
                {
                    totalExtentCount += extents.Count;
                }
                else
                {
                    return new MovingBlocks(
                        iterationKey,
                        ingestedBlockIds
                        .Where(id => id < blockId)
                        .Select(id => ingestedExtentsByBlockId[id])
                        .SelectMany(m => m)
                        .ToImmutableArray());
                }
            }

            return new MovingBlocks(
                iterationKey,
                ingestedExtentsByBlockId.Values
                .SelectMany(m => m)
                .ToImmutableArray());
        }

        private async Task MoveAsync(MovingBlocks movingBlocks, CancellationToken ct)
        {
            var iterationKey = movingBlocks.IterationKey;
            var tempTableName = GetTempTable(iterationKey).TempTableName;
            var destinationTable = RunnerParameters.Parameterization.Activities[iterationKey.ActivityName]
                .GetDestinationTableIdentity();
            var commandClient = RunnerParameters.DbClientFactory.GetDbCommandClient(
                destinationTable.ClusterUri,
                destinationTable.DatabaseName);
            var extentCount = await commandClient.MoveExtentsAsync(
                new KustoPriority(iterationKey),
                tempTableName,
                destinationTable.TableName,
                movingBlocks.Extents
                .Select(e => e.ExtentId),
                ct);
            var blockIds = movingBlocks.Extents
                .Select(e => e.BlockKey.BlockId)
                .Distinct();
            var blocks = RunnerParameters.Database.Blocks.Query()
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

        private void CommitMove(IterationKey iterationKey, IEnumerable<BlockRecord> blocks)
        {
            using (var tx = RunnerParameters.Database.Database.CreateTransaction())
            {
                RunnerParameters.Database.Blocks.Query(tx)
                    .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        b => b.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();
                RunnerParameters.Database.Extents.Query(tx)
                    .Where(pf => pf.Equal(e => e.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        e => e.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();
                RunnerParameters.Database.BlobUrls.Query(tx)
                    .Where(pf => pf.Equal(u => u.BlockKey.IterationKey, iterationKey))
                    .Where(pf => pf.In(
                        u => u.BlockKey.BlockId,
                        blocks.Select(b => b.BlockKey.BlockId)))
                    .Delete();

                RunnerParameters.Database.Blocks.AppendRecords(
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