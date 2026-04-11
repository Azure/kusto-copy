using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class StartCommandRunnerBase : RunnerBase
    {
        #region Inner Types
        protected record CapacityCache(DateTime CachedTime, int CachedCapacity);
        #endregion

        private const int BLOCK_BATCH = 20;
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(2);

        public StartCommandRunnerBase(RunnerParameters parameters, TimeSpan wakePeriod)
           : base(parameters, wakePeriod)
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            var cacheMap = new Dictionary<Uri, CapacityCache>();

            while (!AreActivitiesCompleted())
            {
                var activityNames = Database.Activities.Query()
                    .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                    .Select(a => a.ActivityName)
                    .ToHashSet();
                //  Grouping activities by cluster URI
                var groupings = Parameterization.Activities
                    .Where(a => activityNames.Contains(a.ActivityName))
                    .GroupBy(a => GetClusterUri(a))
                    .Select(g => new
                    {
                        ClusterUri = g.Key,
                        ActivityNames = g
                        .Select(a => a.ActivityName)
                        .OrderBy(n => n)
                        .ToArray()
                    })
                    .ToDictionary(g => g.ClusterUri);

                //  Ensures cached capacity of source cluster
                foreach (var grouping in groupings.Values)
                {
                    if (!cacheMap.ContainsKey(grouping.ClusterUri)
                        || cacheMap[grouping.ClusterUri].CachedTime
                        + CAPACITY_REFRESH_PERIOD < DateTime.Now)
                    {
                        cacheMap[grouping.ClusterUri] = new CapacityCache(
                            DateTime.Now,
                            await FetchCapacityAsync(grouping.ClusterUri, ct));
                    }
                }
                //  Retired unused capacity
                foreach (var clusterUri in cacheMap.Keys)
                {
                    if (!groupings.ContainsKey(clusterUri))
                    {
                        cacheMap.Remove(clusterUri);
                    }
                }

                //  Run each cluster in parallel
                var tasks = groupings
                    .Values
                    .Select(g => Task.Run(() => RunClusterAsync(
                        g.ClusterUri,
                        cacheMap[g.ClusterUri].CachedCapacity,
                        g.ActivityNames,
                        ct)))
                    .ToArray();

                await Task.WhenAll(tasks);
                await SleepAsync(ct);
            }
        }

        protected abstract BlockState InitialState { get; }

        protected abstract BlockState DestinationState { get; }

        protected abstract int? MaxCapacity { get; }

        protected abstract Uri GetClusterUri(ActivityParameterization activity);

        protected abstract Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct);

        private async Task RunClusterAsync(
            Uri clusterUri,
            int capacity,
            string[] activityNames,
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
            string[] activityNames,
            CancellationToken ct)
        {
            var blocks = FetchBlocks(activityNames, capacity);

            if (blocks.Count > 0)
            {
                var iterationKey = blocks.First().BlockKey.IterationKey;
                var activityParam = Parameterization.GetActivity(iterationKey.ActivityName);
                var startExportTasks = blocks
                    .Select(b => Task.Run(() => StartBlockAsync(b, activityParam, ct)))
                    .ToImmutableArray();

                await Task.WhenAll(startExportTasks);

                var newBlocks = startExportTasks.Select(t => t.Result);
                var blockIds = blocks.Select(b => b.BlockKey.BlockId);

                using (var tx = Database.CreateTransaction())
                {
                    Database.Blocks.Query(tx)
                        .Where(pf => pf.Equal(b => b.BlockKey.IterationKey, iterationKey))
                        .Where(pf => pf.In(b => b.BlockKey.BlockId, blockIds))
                        .Delete();
                    Database.Blocks.AppendRecords(newBlocks, tx);

                    tx.Complete();
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