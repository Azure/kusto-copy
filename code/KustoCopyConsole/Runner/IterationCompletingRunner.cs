using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Kusto.Data;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class IterationCompletingRunner : RunnerBase
    {
        public IterationCompletingRunner(
           MainJobParameterization parameterization,
           RowItemGateway rowItemGateway,
           DbClientFactory dbClientFactory)
           : base(parameterization, rowItemGateway, dbClientFactory, TimeSpan.FromSeconds(10))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                CompleteIterations();
                CompleteActivities();

                //  Sleep
                await SleepAsync(ct);
            }
        }

        private void CompleteIterations()
        {
            var completingIterations = RowItemGateway.InMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State != ActivityState.Completed)
                .SelectMany(a => a.IterationMap.Values)
                //  The iteration is planned, hence all its blocks are in place
                .Where(i => i.RowItem.State == IterationState.Planned)
                //  All blocks in the iteration are moved
                .Where(i => !i.BlockMap.Values.Any(b => b.RowItem.State != BlockState.ExtentMoved))
                .Select(i => i.RowItem);

            foreach (var iteration in completingIterations)
            {
                var newIteration = iteration.ChangeState(IterationState.Completed);

                RowItemGateway.Append(newIteration);
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