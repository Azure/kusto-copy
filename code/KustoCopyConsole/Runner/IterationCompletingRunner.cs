using Azure.Core;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class IterationCompletingRunner : RunnerBase
    {
        public IterationCompletingRunner(
            MainJobParameterization parameterization,
            TokenCredential credential,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider)
           : base(
                 parameterization,
                 credential,
                 rowItemGateway,
                 dbClientFactory,
                 stagingBlobUriProvider,
                 TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                await CompleteIterationsAsync(ct);
                CompleteActivities();

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private async Task CompleteIterationsAsync(CancellationToken ct)
        {
            var completingIterations = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                //  The iteration is planned, hence all its blocks are in place
                .Where(i => i.RowItem.State == IterationState.Planned)
                //  All blocks in the iteration are moved
                .Where(i => !i.BlockMap.Any()
                || !i.BlockMap.Values.Any(b => b.RowItem.State != BlockState.ExtentMoved));

            foreach (var iteration in completingIterations)
            {
                if (iteration.TempTable != null
                    && iteration.TempTable.State == TempTableState.Created)
                {
                    var tableId = RowItemGateway.InMemoryCache
                        .ActivityMap[iteration.RowItem.ActivityName]
                        .RowItem
                        .DestinationTable;
                    var dbClient = DbClientFactory.GetDbCommandClient(
                        tableId.ClusterUri,
                        tableId.DatabaseName);

                    await dbClient.DropTableIfExistsAsync(
                        new KustoPriority(iteration.RowItem.GetIterationKey()),
                        iteration.TempTable.TempTableName,
                        ct);
                    await DeleteStorageAsync(ct);
                }
                var newIteration = iteration.RowItem.ChangeState(IterationState.Completed);

                RowItemGateway.Append(newIteration);
            }
        }

        private async Task DeleteStorageAsync(CancellationToken ct)
        {
            await Task.CompletedTask;
            //Parameterization.GetCredentials();
            throw new NotImplementedException();
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