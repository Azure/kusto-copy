using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class BlockCompletingRunner : RunnerBase
    {
        public BlockCompletingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(20))
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

                await TaskHelper.WhenAllWithErrors(directoryDeleteTasks);

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

                        Trace.TraceInformation(
                            $"Deleting Block IDs:  {string.Join(",", blockIds)}");
                    }

                    tx.Complete();
                }

                ValidateUrls();
                await SleepAsync(ct);
            }
        }

        [Conditional("DEBUG")]
        private void ValidateUrls()
        {
            using (var tx = Database.CreateTransaction())
            {
                var blockKeys = Database.BlobUrls.Query(tx)
                    .AsEnumerable()
                    .CountBy(u => u.BlockKey)
                    .ToImmutableDictionary();
                var blockIdsByIterationKey = blockKeys.Keys
                    .GroupBy(k => k.IterationKey)
                    .ToImmutableDictionary(
                    g => g.Key,
                    g => g.Select(i => i.BlockId).ToImmutableArray());

                foreach (var iterationKey in blockIdsByIterationKey.Keys)
                {
                    var blockIdsFromUrls = blockIdsByIterationKey[iterationKey];
                    var blockIdsFromBlocks = Database.Blocks.Query(tx)
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIdsFromUrls))
                        .Select(b => b.BlockKey.BlockId)
                        .ToImmutableArray();
                    var extraBlockIds = blockIdsFromUrls
                        .Except(blockIdsFromBlocks)
                        .ToImmutableArray();

                    if (extraBlockIds.Length > 0)
                    {
                        throw new InvalidOperationException("Dangling URLs of deleted Blocks");
                    }
                }

                tx.CompleteAsync();
            }
        }
    }
}