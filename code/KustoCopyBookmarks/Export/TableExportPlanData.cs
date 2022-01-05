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

        public IImmutableList<TableExportStepData> Steps { get; set; } =
            ImmutableArray<TableExportStepData>.Empty;
    }
}