namespace KustoCopyBookmarks.Export
{
    public class IterationData
    {
        /// <summary>Used only for empty table schema updates.</summary>
        public DateTime IterationTime { get; set; } = DateTime.MinValue;

        /// <summary>Can be null for backfill only.</summary>
        public string? StartCursor { get; set; } = string.Empty;
        
        public string EndCursor { get; set; } = string.Empty;
    }
}