using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class BlockCompletingRunner : RunnerBase
    {
        public BlockCompletingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(15))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {   //  All activity / iteration
                var movedBlockKeys = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.State, BlockState.ExtentMoved))
                    .Take(1000)
                    .Select(b => b.BlockKey)
                    .ToImmutableArray();
                var directoryDeleteTasks = movedBlockKeys
                    .Select(key => StagingBlobUriProvider.DeleteStagingDirectoryAsync(key, ct))
                    .ToImmutableArray();
                var blocksByIterationKey = movedBlockKeys
                    .GroupBy(key => key.IterationKey);

                await Task.WhenAll(directoryDeleteTasks);

                using (var tx = Database.CreateTransaction())
                {
                    foreach (var g in blocksByIterationKey)
                    {
                        var blockIds = g
                            .Select(k => k.BlockId);
                        var deletedBlockCount = Database.Blocks.Query(tx)
                            .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, g.Key))
                            .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                            .Delete();
                        var deletedBlockUrlCount = Database.BlobUrls.Query(tx)
                            .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, g.Key))
                            .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                            .Delete();
                        var deletedExtentCount = Database.Extents.Query(tx)
                            .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, g.Key))
                            .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                            .Delete();
                        var deletedBlockIngestionBatchCount = Database.IngestionBatches.Query(tx)
                            .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, g.Key))
                            .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                            .Delete();
                    }

                    tx.Complete();
                }

                await SleepAsync(ct);
            }
        }
    }
}