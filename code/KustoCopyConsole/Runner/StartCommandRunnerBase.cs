using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class StartCommandRunnerBase : ClusterRunnerBase
    {
        #region Inner Types
        protected record CapacityCache(DateTime CachedTime, int CachedCapacity);
        #endregion

        private const int BLOCK_BATCH = 20;
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(2);

        private readonly ConcurrentDictionary<Uri, CapacityCache> _cacheMap = new();

        protected StartCommandRunnerBase(
            RunnerParameters parameters,
            bool bySourceCluster,
            TimeSpan wakePeriod)
           : base(parameters, bySourceCluster, wakePeriod)
        {
        }

        protected override async Task RunClusterAsync(
            Uri clusterUri,
            IEnumerable<string> activityNames,
            CancellationToken ct)
        {
            //  Ensures cached capacity of cluster
            if (!_cacheMap.ContainsKey(clusterUri)
                || _cacheMap[clusterUri].CachedTime + CAPACITY_REFRESH_PERIOD < DateTime.Now)
            {
                _cacheMap[clusterUri] = new CapacityCache(
                    DateTime.Now,
                    await FetchCapacityAsync(clusterUri, ct));
            }

            await RunClusterAsync( 
                clusterUri,
                _cacheMap[clusterUri].CachedCapacity,
                activityNames,
                ct);
        }

        protected abstract BlockState InitialState { get; }

        protected abstract BlockState DestinationState { get; }

        protected abstract int? MaxCapacity { get; }

        protected abstract Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct);

        private async Task RunClusterAsync(
            Uri clusterUri,
            int capacity,
            IEnumerable<string> activityNames,
            CancellationToken ct)
        {
            while (await RunBatchAsync(
                clusterUri,
                capacity,
                activityNames,
                ct))
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private async Task<bool> RunBatchAsync(
            Uri clusterUri,
            int capacity,
            IEnumerable<string> activityNames,
            CancellationToken ct)
        {
            var blocks = FetchBlocks(activityNames, capacity);

            if (blocks.Count > 0)
            {
                var iterationKey = blocks.First().BlockKey.IterationKey;
                var activityParam = Parameterization.GetActivity(iterationKey.ActivityName);

                foreach (var b in blocks)
                {
                    var newBlock = await StartBlockAsync(b, activityParam, ct);

                    newBlock = newBlock with { State = DestinationState };
                    Database.Blocks.UpdateRecord(b, newBlock);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private IReadOnlyList<BlockRecord> FetchBlocks(
            IEnumerable<string> activityNames,
            int capacity)
        {   //  The first (by priority) block will determine the activity and iteration
            //  we'll work on
            var firstBlock = Database.Blocks.Query()
                .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                .Where(pf => pf.Equal(b => b.State, InitialState))
                .OrderBy(b => b.BlockKey.IterationKey.IterationId)
                .ThenBy(b => b.BlockKey.IterationKey.ActivityName)
                .ThenBy(b => b.BlockKey.BlockId)
                .Take(1)
                .FirstOrDefault();

            if (firstBlock != null)
            {
                //  Fetch all blocks being in destination state (to weight against capacity)
                var destinationCount = (int)Database.Blocks.Query()
                    .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                    .Where(pf => pf.Equal(b => b.State, DestinationState))
                    .Count();
                //  Cap the blocks with override capacity
                var maxCapacity = MaxCapacity;
                var cappedCachedCapacity = maxCapacity == null
                    ? capacity
                    : Math.Min(capacity, maxCapacity.Value);
                //  Cap the blocks with available capacity
                var maxBlockCount =
                    Math.Min(BLOCK_BATCH, Math.Max(0, cappedCachedCapacity - destinationCount));
                var blocks = Database.Blocks.Query()
                    .Where(pf => pf.Equal(
                        b => b.BlockKey.IterationKey,
                        firstBlock.BlockKey.IterationKey))
                    .Where(pf => pf.Equal(b => b.State, InitialState))
                    .OrderBy(b => b.BlockKey.BlockId)
                    .Take(maxBlockCount)
                    .ToArray();

                return blocks;
            }
            else
            {
                return Array.Empty<BlockRecord>();
            }
        }

        protected abstract Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct);
    }
}