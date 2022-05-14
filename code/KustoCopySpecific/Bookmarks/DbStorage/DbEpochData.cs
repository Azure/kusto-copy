using System.Text.Json.Serialization;

namespace KustoCopySpecific.Bookmarks.DbStorage
{
    /// <summary>Represents a replication iteration at the database level.</summary>
    public class DbEpochData
    {
        [JsonIgnore]
        public bool IsBackfill => StartCursor == null;

        /// <summary>Time at which the epoch started (Kusto time).</summary>
        public DateTime EpochStartTime { get; set; } = DateTime.MinValue;

        /// <summary>Can be null for backfill only.</summary>
        public string? StartCursor { get; set; } = null;

        public string EndCursor { get; set; } = string.Empty;

        public bool AllIterationsPlanned { get; set; } = false;
        
        public bool AllIterationsExported { get; set; } = false;
    }
}