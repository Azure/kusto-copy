using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class ActivityCompletingRunner : RunnerBase
    {
        public ActivityCompletingRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                CompleteActivities();

                await SleepAsync(ct);
            }
        }

        private void CompleteActivities()
        {
            var completedIterations = Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.State, IterationState.Completed))
                .ToArray();

            foreach (var iteration in completedIterations)
            {
                var activityParam =
                    Parameterization.GetActivity(iteration.IterationKey.ActivityName);

                if (Parameterization.IterationPeriod == null
                    || Parameterization.CopyMode == CopyMode.BackfillOnly)
                {
                    var activity = Database.Activities.Query()
                        .Where(pf => pf.Equal(
                            a => a.ActivityName,
                            iteration.IterationKey.ActivityName))
                        .First();

                    Database.Activities.UpdateRecord(
                        activity,
                        activity with
                        {
                            State = ActivityState.Completed
                        });
                }
            }
        }
    }
}