using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Db.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
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
            DbClientFactory dbClientFactory,
            AzureBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 database,
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
                    using (var tx = Database.Database.CreateTransaction())
                    {
                        var activity = Database.Activities.Query(tx)
                            .Where(pf => pf.Equal(
                                a => a.ActivityName,
                                iteration.IterationKey.ActivityName))
                            .First();

                        Database.Activities.Query(tx)
                            .Where(pf => pf.Equal(
                                a => a.ActivityName,
                                iteration.IterationKey.ActivityName))
                            .Delete();
                        Database.Activities.AppendRecord(
                            activity with
                            {
                                State = ActivityState.Completed
                            },
                            tx);

                        tx.Complete();
                    }
                }
            }
        }
    }
}