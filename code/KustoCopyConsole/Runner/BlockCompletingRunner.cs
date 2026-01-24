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
            {
                var movedBlockKeys = Database.Blocks.Query()
                    .Where(pf => pf.Equal(b => b.State, BlockState.ExtentMoved))
                    .Take(100)
                    .Select(b => b.BlockKey)
                    .ToImmutableArray();

                if (movedBlockKeys.Any())
                {
                    var directoryDeleteTasks = movedBlockKeys
                        .Select(key => StagingBlobUriProvider.DeleteStagingDirectoryAsync(key, ct))
                        .ToImmutableArray();
                    var iterationGroups = movedBlockKeys
                        .GroupBy(key => key.IterationKey);

                    await Task.WhenAll(directoryDeleteTasks);

                    foreach (var group in iterationGroups)
                    {
                        var blockIds = group
                            .Select(k => k.BlockId);
                        var deletedBlockCount = Database.Blocks.Query()
                            .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, group.Key))
                            .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                            .Delete();
                    }
                }

                await SleepAsync(ct);
            }
        }
    }
}