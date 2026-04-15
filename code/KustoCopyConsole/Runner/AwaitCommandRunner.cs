using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class AwaitCommandRunner : RunnerBase
    {
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

        public AwaitCommandRunner(RunnerParameters parameters, TimeSpan wakePeriod)
           : base(parameters, wakePeriod)
        {
        }

        protected abstract BlockState InitialState { get; }

        protected abstract BlockState ResetState { get; }

        protected abstract Uri GetClusterUri(ActivityParameterization activity);

        protected abstract BlockRecord ResetBlock(BlockRecord block);

        protected abstract string GetOperationId(BlockRecord block);

        protected abstract Task ProcessOperationAsync(
            IEnumerable<BlockRecord> blocks,
            ActivityParameterization activityParam,
            CancellationToken ct);

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AreActivitiesCompleted())
            {
                var activityNames = Database.Activities.Query()
                    .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                    .Select(a => a.ActivityName)
                    .ToHashSet();
                var tasks = Parameterization.Activities
                    .Where(a => activityNames.Contains(a.ActivityName))
                    .GroupBy(a => GetClusterUri(a))
                    .Select(g => Task.Run(() => RunClusterAsync(
                        g.Key,
                        g.Select(a => a.ActivityName).ToArray(),
                        ct)))
                    .ToArray();

                await Task.WhenAll(tasks);
                await SleepAsync(ct);
            }
        }

        private async Task RunClusterAsync(
            Uri clusterUri,
            string[] activityNames,
            CancellationToken ct)
        {
            var blocks = Database.Blocks.Query()
                .Where(pf => pf.In(b => b.BlockKey.IterationKey.ActivityName, activityNames))
                .Where(pf => pf.Equal(b => b.State, InitialState))
                .Take(MAX_OPERATIONS)
                .ToArray();

            if (blocks.Length > 0)
            {
                await UpdateOperationsAsync(clusterUri, blocks, ct);
            }
        }

        private async Task UpdateOperationsAsync(
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

            DetectLostOperationIds(operationIdMap, statuses);
            DetectFailures(operationIdMap, statuses);
            await CompleteOperationsAsync(operationIdMap, statuses, ct);
        }

        #region Handle Operations
        private void DetectLostOperationIds(
            IDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<OperationStatus> status)
        {
            var statusOperationIdBag = status
                .Select(s => s.OperationId)
                .ToHashSet();

            foreach (var id in operationIdMap.Keys)
            {
                if (!statusOperationIdBag.Contains(id))
                {
                    var block = operationIdMap[id];

                    Database.Blocks.UpdateRecord(
                        block,
                        ResetBlock(block) with
                        {
                            State = ResetState
                        });
                    TraceWarning(
                        $"Warning!  Operation ID lost:  '{id}' for " +
                        $"block {block.BlockKey} in state {block.State} ; " +
                        "block marked for reprocessing");
                }
            }
        }

        private void DetectFailures(
            IDictionary<string, BlockRecord> operationIdMap,
            IImmutableList<OperationStatus> statuses)
        {
            var failedStatuses = statuses
                .Where(s => FAILED_STATUS.Contains(s.State));

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
                    Database.Blocks.UpdateRecord(
                        block,
                        ResetBlock(block) with
                        {
                            State = ResetState
                        });
                }
                else
                {
                    throw new CopyException($"Permanent {block.State} error", false);
                }
            }
        }

        private async Task CompleteOperationsAsync(
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
        }
        #endregion
    }
}