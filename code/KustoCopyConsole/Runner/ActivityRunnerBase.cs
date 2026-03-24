using KustoCopyConsole.Entity.State;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class ActivityRunnerBase : RunnerBase
    {
        public ActivityRunnerBase(RunnerParameters parameters, TimeSpan wakePeriod)
            : base(parameters, wakePeriod)
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AreActivitiesCompleted())
            {
                var activityNames = Database.Activities.Query()
                    .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                    .Select(a => a.ActivityName)
                    .ToImmutableArray();

                foreach (var activityName in activityNames)
                {
                    await RunActivityAsync(activityName, ct);
                }
                await SleepAsync(ct);
            }
        }

        protected abstract Task RunActivityAsync(string activityName, CancellationToken ct);
    }
}