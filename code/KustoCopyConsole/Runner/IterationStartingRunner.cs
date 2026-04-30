using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using System;
using System.Linq;
using TrackDb.Lib;

namespace KustoCopyConsole.Runner
{
    internal class IterationStartingRunner : RunnerBase
    {
        public IterationStartingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(30))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted() && StartIterations())
            {
            }
        }

        /// <summary>
        /// Returns <c>true</c> iif all iterations that should be created in the current process
        /// run were created.
        /// </summary>
        /// <returns></returns>
        private bool StartIterations()
        {
            using (var tx = Database.CreateTransaction())
            {
                var activityNames = Parameterization.Activities.Select(a => a.ActivityName);
                var allIterationsByActivityName = Database.Iterations.Query(tx)
                    .Where(pf => pf.In(t => t.IterationKey.ActivityName, activityNames))
                    .GroupBy(i => i.IterationKey.ActivityName)
                    .ToDictionary(g => g.Key, g => g.OrderBy(i => i.IterationKey.IterationId).ToArray());
                var hasActiveIteration = allIterationsByActivityName.Values
                    .SelectMany(a => a)
                    .Any(i => i.State != IterationState.Completed);

                foreach (var activityName in activityNames)
                {
                    var iterations = allIterationsByActivityName.ContainsKey(activityName)
                        ? allIterationsByActivityName[activityName]
                        : Array.Empty<IterationRecord>();

                    if (NeedNewIteration(activityName, iterations, hasActiveIteration, tx))
                    {
                        var lastIteration = iterations.LastOrDefault();
                        var newIterationId = lastIteration != null
                            ? lastIteration.IterationKey.IterationId + 1
                            : 1;
                        var cursorStart = lastIteration != null
                            ? lastIteration.CursorEnd
                            : string.Empty;
                        var newIterationRecord = new IterationRecord(
                            IterationState.Starting,
                            new IterationKey(activityName, newIterationId),
                            cursorStart,
                            string.Empty,
                            0,
                            DateTime.Now);

                        Database.Iterations.AppendRecord(newIterationRecord, tx);
                    }
                }

                tx.Complete();
            }

            return Parameterization.CopyMode == CopyMode.BackfillOnly
                || Parameterization.IterationPeriod == null;
        }

        private bool NeedNewIteration(
            string activityName,
            IterationRecord[] iterations,
            bool hasActiveIteration,
            TransactionContext tx)
        {
            if (iterations.Length == 0)
            {   //  No iteration:  we need to create a new one
                return true;
            }
            else if (Parameterization.CopyMode != CopyMode.BackfillOnly)
            {
                if (hasActiveIteration && Parameterization.IterationPeriod == null)
                {
                    return false;
                }
                else
                {
                    var incompletedIterations = iterations
                        .Where(i => i.State != IterationState.Completed);

                    return incompletedIterations.Count() < 2
                        && (incompletedIterations.LastOrDefault() == null
                        || incompletedIterations.Last().StartTime
                        + Parameterization.IterationPeriod > DateTime.Now);
                }
            }
            else
            {
                return false;
            }
        }
    }
}