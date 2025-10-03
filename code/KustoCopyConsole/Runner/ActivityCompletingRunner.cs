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
           : base(parameters, TimeSpan.FromSeconds(5))
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
                .ToImmutableArray();

            foreach (var iteration in completedIterations)
            {
                var activityParam = Parameterization.Activities[iteration.IterationKey.ActivityName];

                if (!Parameterization.IsContinuousRun
                    || activityParam.TableOption.ExportMode == ExportMode.BackfillOnly
                    || activityParam.TableOption.ExportMode == ExportMode.NewOnly)
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