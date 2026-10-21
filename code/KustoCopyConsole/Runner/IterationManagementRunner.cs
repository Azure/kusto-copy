using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System;
using System.Linq;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class IterationManagementRunner : RunnerBase
    {
        public IterationManagementRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(20))
        {
        }

        public async override Task RunAsync(CancellationToken ct)
        {
            var isProcessStarting = true;

            while (ShouldRunnersContinue())
            {
                await ManageIterationsAsync(isProcessStarting, ct);
                isProcessStarting = false;

                await SleepAsync(ct);
            }
        }

        private async Task ManageIterationsAsync(bool isProcessStarting, CancellationToken ct)
        {
            var activityNames = Parameterization.Activities.Select(a => a.ActivityName);
            var allIterationsByActivityName = Database.Iterations.Query()
                .Where(pf => pf.In(t => t.IterationKey.ActivityName, activityNames))
                .GroupBy(i => i.IterationKey.ActivityName)
                .ToDictionary(g => g.Key, g => g.OrderBy(i => i.IterationKey.IterationId));
            var hasActiveIteration = allIterationsByActivityName.Values
                .SelectMany(a => a)
                .Any(i => i.State != IterationState.Completed);

            foreach (var activityName in activityNames)
            {
                var iterations = allIterationsByActivityName.ContainsKey(activityName)
                    ? allIterationsByActivityName[activityName].ToArray()
                    : Array.Empty<IterationRecord>();

                CreateIteration(
                    activityName,
                    iterations,
                    isProcessStarting,
                    hasActiveIteration);
                await CleanIterationArtefactsAsync(iterations, ct);
                using (var tx = Database.CreateTransaction())
                {
                    CleanIterations(activityName, iterations, tx);
                    CompleteActivity(activityName, iterations, tx);

                    tx.Complete();
                }
            }
        }

        #region Create Iteration
        private void CreateIteration(
            string activityName,
            IterationRecord[] iterations,
            bool isProcessStarting,
            bool hasActiveIteration)
        {
            if (ShouldCreateIteration(iterations, isProcessStarting, hasActiveIteration))
            {
                var lastIteration = iterations.LastOrDefault();
                var newIterationId = lastIteration != null
                    ? lastIteration.IterationKey.IterationId + 1
                    : 1;
                var cursorStart = lastIteration != null
                    ? lastIteration.CursorEnd
                    : string.Empty;
                var newIteration = new IterationRecord(
                    IterationState.Starting,
                    new IterationKey(activityName, newIterationId),
                    cursorStart,
                    string.Empty,
                    0,
                    DateTime.Now);

                Database.Iterations.AppendRecord(newIteration);
            }
        }

        private bool ShouldCreateIteration(
            IterationRecord[] iterations,
            bool isProcessStarting,
            bool hasActiveIteration)
        {
            if (iterations.Length == 0)
            {   //  No iterations
                return true;
            }
            else if (Parameterization.CopyMode != CopyMode.BackfillOnly)
            {
                if (isProcessStarting && !hasActiveIteration)
                {   //  New process and no active iterations (overall)
                    return true;
                }
                else if (Parameterization.IterationPeriod != null
                    && iterations.Last().StartTime + Parameterization.IterationPeriod <= DateTime.Now
                    && iterations.Count(i => i.State != IterationState.Completed) < 2)
                {   //  Iteration period is due and there is "space" for a new iteration
                    return true;
                }
            }

            return false;
        }
        #endregion

        #region Complete Iteration
        private async Task CleanIterationArtefactsAsync(
            IterationRecord[] iterations,
            CancellationToken ct)
        {
            var plannedIterations = iterations
                .Where(i => i.State == IterationState.Planned);
            var toCompleteIterations = await GetIterationToCompleteAsync(plannedIterations, ct);

            foreach (var iteration in toCompleteIterations)
            {
                var cleanDirectoryTask = CleanStorageDirectoryAsync(iteration, ct);

                await CleanTempTableAsync(iteration, ct);
                await cleanDirectoryTask;
            }
            using (var tx = Database.CreateTransaction())
            {
                foreach (var iteration in toCompleteIterations)
                {
                    CompleteIteration(iteration, tx);
                }

                tx.Complete();
            }
        }

        private void CompleteIteration(IterationRecord iteration, TransactionContext tx)
        {
            Database.TempTables.Query(tx)
                .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                .Delete();
            Database.PlanningPartitions2.Query(tx)
                .Where(pf => pf.Equal(i => i.IterationKey, iteration.IterationKey))
                .Delete();
            Database.Iterations.UpdateRecord(
                iteration,
                iteration with
                {
                    State = IterationState.Completed
                },
                tx);
        }

        private async Task CleanStorageDirectoryAsync(
            IterationRecord iteration,
            CancellationToken ct)
        {
            await StagingBlobUriProvider.DeleteStagingRootDirectoryAsync(
                iteration.IterationKey,
                ct);
        }

        private async Task CleanTempTableAsync(IterationRecord iteration, CancellationToken ct)
        {
            var tempTable = TryGetTempTable(iteration.IterationKey);

            if (tempTable != null)
            {
                var destinationTable = Parameterization
                    .GetActivity(iteration.IterationKey.ActivityName)
                    .GetDestinationTableIdentity();
                var dbClient = DbClientFactory.GetDbCommandClient(
                    destinationTable.ClusterUri,
                    destinationTable.DatabaseName);

                await dbClient.DropTableIfExistsAsync(
                    new KustoPriority(iteration.IterationKey),
                    tempTable.TempTableName,
                    ct);
            }
        }

        private async Task<IEnumerable<IterationRecord>> GetIterationToCompleteAsync(
            IEnumerable<IterationRecord> iterations,
            CancellationToken ct)
        {
            bool IsIterationToComplete(IterationKey iterationKey, TransactionContext tx)
            {
                var pendingBlockCount = Database.QueryAggregatedBlockMetrics(iterationKey, tx)
                    .Where(p => p.Key < BlockMetric.ExtentMoved)
                    .Sum(p => p.Value);

                return pendingBlockCount == 0;
            }

            using (var tx = Database.CreateTransaction())
            {
                var iterationState = iterations
                    .Select(i => new
                    {
                        Iteration = i,
                        IsIterationToComplete = IsIterationToComplete(i.IterationKey, tx)
                    })
                    .ToArray();

                if (iterationState.Any(s => s.IsIterationToComplete))
                {
                    await tx.CompleteAsync();

                    return iterationState
                        .Where(s => s.IsIterationToComplete)
                        .Select(s => s.Iteration);
                }
                else
                {
                    tx.Complete();

                    return Array.Empty<IterationRecord>();
                }
            }
        }
        #endregion

        private void CleanIterations(
            string activityName,
            IterationRecord[] iterations,
            TransactionContext tx)
        {
            var firstActiveIterationIndex = iterations
                .Index()
                .Where(p => p.Item.State != IterationState.Completed)
                .Select(p => p.Index)
                .FirstOrDefault();

            if (firstActiveIterationIndex > 0)
            {
                var iterationIdsToDelete = iterations
                    .Select(i => i.IterationKey.IterationId)
                    .Take(firstActiveIterationIndex);

                Database.Iterations.Query(tx)
                    .Where(pf => pf.Equal(i => i.IterationKey.ActivityName, activityName))
                    .Where(pf => pf.In(i => i.IterationKey.IterationId, iterationIdsToDelete))
                    .Delete();
            }
        }

        private void CompleteActivity(
            string activityName,
            IterationRecord[] iterations,
            TransactionContext tx)
        {
            if (iterations.Length > 0
                && iterations.All(i => i.State == IterationState.Completed)
                && (Parameterization.IterationPeriod == null
                || Parameterization.CopyMode == CopyMode.BackfillOnly))
            {
                var activity = Database.Activities.Query()
                    .Where(pf => pf.Equal(a => a.ActivityName, activityName))
                    .First();

                Database.Activities.UpdateRecord(
                    activity,
                    activity with
                    {
                        State = ActivityState.Completed
                    },
                    tx);
            }
        }
    }
}