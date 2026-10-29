using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class StartAwaitCommandRunnerBase : ClusterRunnerBase
    {
        #region Inner Types
        protected record CapacityCache(DateTime CachedTime, int CachedCapacity);
        #endregion

        private const int MAX_OPERATIONS = 200;
        private static readonly IImmutableSet<string> FAILED_STATUS =
            ImmutableHashSet.Create(
                [
                "Throttled",
                "Failed",
                "PartiallySucceeded",
                "Abandoned",
                "BadInput",
                "Canceled",
                "Skipped"
                ]);

        private const int START_BLOCK_BATCH = 20;
        private static readonly TimeSpan CAPACITY_REFRESH_PERIOD = TimeSpan.FromMinutes(2);

        private readonly ConcurrentDictionary<Uri, CapacityCache> _cacheMap = new();

        protected StartAwaitCommandRunnerBase(
            RunnerParameters parameters,
            bool bySourceCluster,
            TimeSpan wakePeriod)
           : base(parameters, bySourceCluster, wakePeriod)
        {
        }

        protected override async Task RunClusterAsync(
            Uri clusterUri,
            IEnumerable<string> activityNames,
            bool isRunnerShared,
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

            await RunClusterWithCapacityAsync(
                clusterUri,
                _cacheMap[clusterUri].CachedCapacity,
                activityNames,
                ct);
        }

        #region Abstract Properties / Methods
        protected abstract int? MaxCapacity { get; }

        protected abstract BlockState InitialState { get; }

        protected abstract BlockState TransitionState { get; }

        protected abstract void ResetLostBlock(BlockRecord lostBlock);

        protected abstract void RetryFailedBlock(BlockRecord failedBlock, string status);

        protected abstract Task<int> FetchCapacityAsync(Uri clusterUri, CancellationToken ct);

        protected abstract Task<BlockRecord> StartBlockAsync(
            BlockRecord blockRecord,
            ActivityParameterization activityParam,
            CancellationToken ct);

        protected abstract string GetOperationId(BlockRecord block);

        protected abstract Task ProcessOperationAsync(
            IEnumerable<BlockRecord> blocks,
            ActivityParameterization activityParam,
            CancellationToken ct);
        #endregion

        private async Task RunClusterWithCapacityAsync(
            Uri clusterUri,
            int capacity,
            IEnumerable<string> activityNames,
            CancellationToken ct)
        {
            var doReRun = true;

            while (doReRun)
            {
                while (await RunStartBatchAsync(
                    clusterUri,
                    capacity,
                    activityNames,
                    ct))
                {
                    ct.ThrowIfCancellationRequested();
                }
                var transitionBlocks = Database.Blocks.Query()
                    .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                    .Where(pf => pf.Equal(b => b.State, TransitionState))
                    .Take(MAX_OPERATIONS)
                    .ToArray();

                doReRun = transitionBlocks.Length > 0
                    && await UpdateOperationsAsync(clusterUri, transitionBlocks, ct);
            }
        }

        #region Start logic
        private async Task<bool> RunStartBatchAsync(
            Uri clusterUri,
            int capacity,
            IEnumerable<string> activityNames,
            CancellationToken ct)
        {
            var blocks = FetchInitialBlocks(activityNames, capacity);

            if (blocks.Count > 0)
            {
                var iterationKey = blocks.First().BlockKey.IterationKey;
                var activityParam = Parameterization.GetActivity(iterationKey.ActivityName);

                foreach (var b in blocks)
                {
                    var newBlock = await StartBlockAsync(b, activityParam, ct);

                    newBlock = newBlock with { State = TransitionState };
                    Database.Blocks.UpdateRecord(b, newBlock);
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        private IReadOnlyList<BlockRecord> FetchInitialBlocks(
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
                //  Fetch all blocks being in transition state (to weight against capacity)
                var transitionCount = (int)Database.Blocks.Query()
                    .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                    .Where(pf => pf.Equal(b => b.State, TransitionState))
                    .Count();
                //  Cap the blocks with override capacity
                var maxCapacity = MaxCapacity;
                var cappedCachedCapacity = maxCapacity == null
                    ? capacity
                    : Math.Min(capacity, maxCapacity.Value);
                //  Cap the blocks with available capacity
                var maxBlockCount =
                    Math.Min(START_BLOCK_BATCH, Math.Max(0, cappedCachedCapacity - transitionCount));
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
        #endregion

        #region Await Logic
        private async Task<bool> UpdateOperationsAsync(
            Uri clusterUri,
            BlockRecord[] blocks,
            CancellationToken ct)
        {
            var dbClient = DbClientFactory.GetDbCommandClient(clusterUri, string.Empty);
            var operationIdMap = blocks
                .ToDictionary(b => GetOperationId(b));
            var statuses = await dbClient.ShowOperationsAsync(
                KustoPriority.HighestPriority,
                operationIdMap.Keys,
                ct);
            var hasLostOperations = ProcessLostOperationIds(operationIdMap, statuses);
            var hasFailures = ProcessFailures(operationIdMap, statuses);
            var hasCompletedOperations =
                await CompleteOperationsAsync(operationIdMap, statuses, ct);

            return hasLostOperations || hasFailures || hasCompletedOperations;
        }

        #region Handle Operations
        private bool ProcessLostOperationIds(
            IDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<OperationStatus> status)
        {
            var statusOperationIdBag = status
                .Select(s => s.OperationId)
                .ToHashSet();
            var hasLostBlocks = false;

            foreach (var id in operationIdMap.Keys)
            {
                if (!statusOperationIdBag.Contains(id))
                {
                    var block = operationIdMap[id];

                    TraceWarning(
                        $"Warning!  Operation ID lost:  '{id}' for " +
                        $"block {block.BlockKey} in state {block.State} ; " +
                        "block marked for reprocessing");
                    ResetLostBlock(block);
                    hasLostBlocks = true;
                }
            }

            return hasLostBlocks;
        }

        private bool ProcessFailures(
            IDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<OperationStatus> statuses)
        {
            var failedStatuses = statuses
                .Where(s => FAILED_STATUS.Contains(s.State));
            var failedBlocks = new List<BlockRecord>();

            foreach (var status in failedStatuses)
            {
                var block = operationIdMap[status.OperationId];
                var message = status.ShouldRetry
                    ? "block marked for reprocessing"
                    : "block can't be reprocessed";
                var warning = $"Warning!  Operation ID in state '{status.State}', " +
                    $"status '{status.Status}' " +
                    $"block {block.BlockKey} in state {block.State} ; {message}";

                TraceWarning(warning);
                if (status.ShouldRetry)
                {
                    RetryFailedBlock(block, status.Status);
                }
                else
                {
                    throw new CopyException($"Permanent {block.State} error", false);
                }
            }

            return failedBlocks.Count > 0;
        }

        private async Task<bool> CompleteOperationsAsync(
            IDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<OperationStatus> statuses,
            CancellationToken ct)
        {
            var blocks = statuses
                .Where(s => s.State == "Completed")
                .Select(s => operationIdMap[s.OperationId]);
            var blocksByIterationKey = blocks
                .GroupBy(b => b.BlockKey.IterationKey);
            var processTasks = blocksByIterationKey
                .Select(g => ProcessOperationAsync(
                    g,
                    Parameterization.GetActivity(g.Key.ActivityName),
                    ct))
                .ToArray();

            await Task.WhenAll(processTasks);

            return processTasks.Length > 0;
        }
        #endregion
        #endregion
    }
}