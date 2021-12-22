using System.Collections.Immutable;
using System.Text.Json.Serialization;

namespace KustoCopyBookmarks.Export
{
    /// <summary>Represents a replication iteration at the table level.</summary>
    public class TableIterationData
    {
        /// <summary>Identify the iteration.</summary>
        public string EndCursor { get; set; } = string.Empty;

        public string TableName { get; set; } = string.Empty;

        public IImmutableList<DateTime> RemainingDayIngestionTimes { get; set; }
            = ImmutableArray<DateTime>.Empty;

        public DateTime? MinRemainingIngestionTime { get; set; }
        
        public DateTime? MaxRemainingIngestionTime { get; set; }
    }
}