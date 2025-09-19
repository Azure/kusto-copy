using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class ActivityRunnerBase : RunnerBase
    {
        public ActivityRunnerBase(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            DbClientFactory dbClientFactory,
            AzureBlobUriProvider stagingBlobUriProvider,
            TimeSpan wakePeriod)
            : base(
                 parameterization,
                 credential,
                 database,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 wakePeriod)
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            await TaskHelper.WhenAllWithErrors(Parameterization.Activities.Keys
                .Select(a => Task.Run(() => RunActivityLoopAsync(a, ct))));
        }

        protected abstract Task<bool> RunActivityAsync(string activityName, CancellationToken ct);

        private async Task RunActivityLoopAsync(string activityName, CancellationToken ct)
        {
            while (!IsActivityCompleted(activityName))
            {
                if (await RunActivityAsync(activityName, ct))
                {
                    await SleepAsync(ct);
                }
            }
        }

        private bool IsActivityCompleted(string activityName)
        {
            var isCompleted = Database.Activities.Query()
                .Where(pf => pf.Equal(a => a.ActivityName, activityName))
                .Where(pf => pf.Equal(a => a.State, ActivityState.Completed))
                .Count() == 1;

            return isCompleted;
        }
    }
}