using Kusto.Cloud.Platform.Utils;
using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameter
{
    internal class SourceClusterParameterization
    {
        public bool IsContinuousRun { get; set; } = false;
     
        public string SourceClusterUri { get; set; } = string.Empty;

        public ExportMode ExportMode { get; set; } = ExportMode.BackFillAndNew;

        public TimeSpan IterationWait { get; set; } = TimeSpan.FromMinutes(5);

        public int ConcurrentQueryCount { get; set; } = 0;

        public int ConcurrentExportCommandCount { get; set; } = 0;

        public IImmutableList<SourceDatabaseParameterization> Databases { get; set; } =
            ImmutableArray<SourceDatabaseParameterization>.Empty;

        public void Validate()
        {
        }
    }
}