using Kusto.Data.Common;
using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace KustoCopyConsole.Storage
{
    public class ExportRecordBatchState
    {
        public IImmutableList<TableColumn>? TableColumns { get; set; } = null;

        public IImmutableList<Uri> BlobPaths { get; set; }
            = ImmutableArray<Uri>.Empty;

        public long RecordCount { get; set; }

        [JsonConverter(typeof(JsonStringEnumConverter))]
        public ExportFormat ExportFormat { get; set; } = ExportFormat.CsvGz;
    }
}