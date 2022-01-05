using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class TableExportStepData
    {
        public IImmutableList<DateTime> IngestionTimes { get; set; }
            = ImmutableArray<DateTime>.Empty;

        public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
    }
}