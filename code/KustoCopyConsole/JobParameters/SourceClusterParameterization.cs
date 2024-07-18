using Kusto.Cloud.Platform.Utils;
using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameters
{
    internal class SourceClusterParameterization
    {
        public string SourceClusterUri { get; set; } = string.Empty;

        public ExportMode ExportMode { get; set; } = ExportMode.BackFillAndNew;

        public TimeSpan IterationWait { get; set; } = TimeSpan.FromMinutes(5);

        public int ConcurrentQueryCount { get; set; } = 0;

        public int ConcurrentExportCommandCount { get; set; } = 0;

        public IImmutableList<SourceDatabaseParameterization> Databases { get; set; } =
            ImmutableArray<SourceDatabaseParameterization>.Empty;
    }
}