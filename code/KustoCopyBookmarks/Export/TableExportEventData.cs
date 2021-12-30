using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class TableExportEventData : EmptyTableExportEventData
    {
        public string TempFolderUrl { get; set; } = "<EMPTY?>";

        public IImmutableList<DateTime> IngestionTimes { get; set; } = ImmutableArray<DateTime>.Empty;

        public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
    }
}