namespace KustoCopyConsole.Storage
{
    public class SubIterationState
    {
        public DateTime? StartIngestionTime { get; set; }
        
        public DateTime? EndIngestionTime { get; set; }

        public string? StagingTableSuffix { get; set; }
    }
}