using System.Collections.Immutable;

namespace KustoCopySpecific.Parameters
{
    public class CompleteDatabaseParameterization
    {
        public string ClusterQueryUri { get; set; } = string.Empty;

        public int ConcurrentQueryCount { get; set; }

        public int ConcurrentExportCommandCount { get; set; }

        public bool IsEnabled { get; set; }

        public long MaxRowsPerTablePerIteration { get; set; }

        public IImmutableList<DestinationParameterization> Destinations { get; set; } =
            ImmutableArray<DestinationParameterization>.Empty;
    }
}