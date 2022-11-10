using System.Collections.Immutable;

namespace KustoCopyConsole.Parameters
{
    public class SourceParameterization
    {
        public string? ClusterQueryConnectionString { get; set; }

        public int ConcurrentQueryCount { get; set; } = 1;

        public int ConcurrentExportCommandCount { get; set; } = 2;

        public DatabaseConfigParameterization DatabaseDefault { get; set; } =
            new DatabaseConfigParameterization();

        public IImmutableList<SourceDatabaseParameterization> Databases { get; set; } =
            ImmutableArray<SourceDatabaseParameterization>.Empty;
    }
}