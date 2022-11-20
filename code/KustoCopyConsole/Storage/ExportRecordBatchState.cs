using Kusto.Data.Common;
using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class ExportRecordBatchState
    {
        public IImmutableList<TableColumn>? TableColumns { get; set; } = null;

        public IImmutableList<Uri> BlobPaths { get; set; }
            = ImmutableArray<Uri>.Empty;

        public IImmutableList<Uri> ExtentIds { get; set; }
            = ImmutableArray<Uri>.Empty;
    }
}