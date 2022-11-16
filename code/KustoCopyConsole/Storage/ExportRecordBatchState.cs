using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class ExportRecordBatchState
    {
        public IImmutableList<Uri> BlobPaths { get; set; }
            = ImmutableArray<Uri>.Empty;

        public IImmutableList<Uri> ExtentIds { get; set; }
            = ImmutableArray<Uri>.Empty;
    }
}