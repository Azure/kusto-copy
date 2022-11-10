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

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ClusterQueryConnectionString))
            {
                throw new CopyException(
                    $"{nameof(ClusterQueryConnectionString)} isn't specified");
            }
            if (!Databases.Any())
            {
                throw new CopyException(
                    $"{nameof(Databases)} should contain at least one database");
            }
            foreach (var d in Databases)
            {
                d.Validate();
            }
        }
    }
}