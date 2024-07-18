using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameters
{
    public class SourceDatabaseParameterization
    {
        public string DatabaseName { get; set; } = string.Empty;

        public IImmutableList<SourceTableParameterization> Tables { get; set; } =
            ImmutableArray<SourceTableParameterization>.Empty;

        public IImmutableList<DestinationParameterization> Destinations { get; set; } =
            ImmutableArray<DestinationParameterization>.Empty;
    }
}