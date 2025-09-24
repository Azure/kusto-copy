using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
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
            var completedIterations = RunnerParameters.Database.Iterations.Query()
                .Where(pf => pf.Equal(i => i.State, IterationState.Completed))
                .ToImmutableArray();

            foreach (var iteration in completedIterations)
            {
                var activityParam = RunnerParameters.Parameterization.Activities[iteration.IterationKey.ActivityName];

                if (!RunnerParameters.Parameterization.IsContinuousRun
                    || activityParam.TableOption.ExportMode == ExportMode.BackfillOnly
                    || activityParam.TableOption.ExportMode == ExportMode.NewOnly)
                {
                    var activity = RunnerParameters.Database.Activities.Query()
                        .Where(pf => pf.Equal(
                            a => a.ActivityName,
                            iteration.IterationKey.ActivityName))
                        .First();

                    RunnerParameters.Database.Activities.UpdateRecord(
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
