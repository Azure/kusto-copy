using System.Collections.Immutable;

namespace KustoCopyBookmarks.IterationExportStorage
{
    public class TableStorageFolderData
    {
        public IImmutableList<string> BlobNames { get; set; } = ImmutableArray<string>.Empty;

        public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;

        public long RowCount { get; set; } = 0;
    }
}