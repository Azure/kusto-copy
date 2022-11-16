using Kusto.Cloud.Platform.Utils;
using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class RecordBatchState
    {
        public IImmutableList<TimeInterval> IngestionTimes { get; set; }
            = ImmutableArray<TimeInterval>.Empty;

        public DateTime? CreationTime { get; set; }

        public long? RecordCount { get; set; }

        public IImmutableList<Uri> BlobPaths { get; set; }
            = ImmutableArray<Uri>.Empty;

        public IImmutableList<Uri> ExtentIds { get; set; }
            = ImmutableArray<Uri>.Empty;
    }
}