﻿using System.Text.Json.Serialization;

namespace KustoCopyBookmarks.Export
{
    /// <summary>Represents a replication iteration at the database level.</summary>
    public class DbIterationData
    {
        [JsonIgnore]
        public bool IsBackfill => StartCursor == null;

        /// <summary>Time at which the iteration started (Kusto time).</summary>
        public DateTime IterationTime { get; set; } = DateTime.MinValue;

        /// <summary>Can be null for backfill only.</summary>
        public string? StartCursor { get; set; } = null;

        public string EndCursor { get; set; } = string.Empty;
    }
}