using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class ActivityCompletingRunner : RunnerBase
    {
        public ActivityCompletingRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(5))
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
            var candidateActivities = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                //  There is at least one iteration:  exclude iteration-less activities
                .Where(a => a.IterationMap.Any())
                //  All iterations are completed
                .Where(a => !a.IterationMap.Values.Any(i => i.RowItem.State != IterationState.Completed))
                .Select(a => a.RowItem);

            foreach (var activity in candidateActivities)
            {
                if (!Parameterization.IsContinuousRun
                    || Parameterization.Activities[activity.ActivityName].TableOption.ExportMode
                    == ExportMode.BackfillOnly)
                {
                    var newActivity = activity.ChangeState(ActivityState.Completed);

                    RowItemGateway.Append(newActivity);
                }
            }
        }
    }
}