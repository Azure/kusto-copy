using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class TableIngestionDays
    {
        public string TableName { get; set; } = "<EMPTY?>";

        public bool IsBackfill { get; set; } = true;

        public IImmutableList<DateTime> IngestionDayTime { get; set; }
            = ImmutableArray<DateTime>.Empty;
    }
}