using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Export
{
    public class TableExportPlanData
    {
        /// <summary>Identify the epoch.</summary>
        public string EpochEndCursor { get; set; } = string.Empty;

        /// <summary>Identify the iteration.</summary>
        public int Iteration { get; set; } = 0;

        public string TableName { get; set; } = string.Empty;

        /// <summary>If this collection is empty, this means the table was empty for this iteration.</summary>
        public IImmutableList<DateTime> IngestionTimes { get; set; } = ImmutableArray<DateTime>.Empty;

        /// <summary>Meaningful only if <see cref="IngestionTimes"/> is non-empty.</summary>
        public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;
    }
}