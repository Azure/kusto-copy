namespace KustoCopyConsole.Parameters
{
    public class DestinationParameterization
    {
        public bool IsEnabled { get; set; } = true;
        
        public string? ClusterQueryConnectionString { get; set; }

        public int ConcurrentQueryCount { get; set; } = 2;
     
        public int ConcurrentIngestionCount { get; set; } = 0;
    }
}