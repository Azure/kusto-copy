using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class ActivityRunnerBase : RunnerBase
    {
        public ActivityRunnerBase(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider,
            TimeSpan wakePeriod)
            : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 wakePeriod)
        {
        }

        public abstract Task RunActivityAsync(string activityName, CancellationToken ct);

        protected bool IsActivityCompleted(string activityName)
        {
            var isCompleted = Database.Activities.Query()
                .Where(pf => pf.Equal(a => a.ActivityName, activityName))
                .Where(pf => pf.Equal(a => a.State, ActivityState.Completed))
                .Count() == 1;

            return isCompleted;
        }
    }
}