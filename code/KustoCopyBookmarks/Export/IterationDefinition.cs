namespace KustoCopyBookmarks.Export
{
    public class IterationDefinition
    {
        /// <summary>Used only for empty table schema updates.</summary>
        public DateTime IterationTime { get; set; } = DateTime.MinValue;

        /// <summary>Can be null for backfill only.</summary>
        public string? Start { get; set; } = string.Empty;
        
        public string End { get; set; } = string.Empty;
    }
}