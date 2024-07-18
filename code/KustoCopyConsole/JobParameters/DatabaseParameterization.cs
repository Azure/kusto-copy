using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameters
{
    public class DatabaseParameterization
    {
        public string Name { get; set; } = string.Empty;

        public IImmutableList<TableParameterization> Tables { get; set; } =
            ImmutableArray<TableParameterization>.Empty;

        public IImmutableList<DestinationParameterization> Destinations { get; set; } =
            ImmutableArray<DestinationParameterization>.Empty;
    }
}