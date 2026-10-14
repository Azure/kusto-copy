using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class ClusterRunnerBase : RunnerBase
    {
        private const int PARALLEL_CALL_PER_CORE_COUNT = 2;
        //  If true, we group by source clusters, if false, we group by destination clusters
        private readonly bool _bySourceCluster;

        protected ClusterRunnerBase(
            RunnerParameters parameters,
            bool bySourceCluster,
            TimeSpan wakePeriod)
            : base(parameters, wakePeriod)
        {
            _bySourceCluster = bySourceCluster;
        }

        public override async Task RunAsync(CancellationToken ct)
        {
            while (ShouldRunnersContinue())
            {
                var parallelCount = Environment.ProcessorCount * PARALLEL_CALL_PER_CORE_COUNT;
                var activityNames = Database.Activities.Query()
                    .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                    .Select(a => a.ActivityName)
                    .ToHashSet();
                //  Grouping activities by cluster URI
                var groupings = Parameterization.Activities
                    .Where(a => activityNames.Contains(a.ActivityName))
                    .GroupBy(a => GetClusterUri(a), a => a.ActivityName);

                await Parallel.ForEachAsync(
                    groupings,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = parallelCount,
                        CancellationToken = ct
                    },
                    async (g, token) =>
                    {
                        await Task.Run(() => RunClusterAsync(g.Key, g, token), token);
                    });
                await SleepAsync(ct);
            }
        }

        protected abstract Task RunClusterAsync(
            Uri clusterUri,
            IEnumerable<string> activityNames,
            CancellationToken ct);

        private Uri GetClusterUri(ActivityParameterization a)
        {
            return _bySourceCluster
                ? a.GetSourceTableIdentity().ClusterUri
                : a.GetDestinationTableIdentity().ClusterUri;
        }
    }
}