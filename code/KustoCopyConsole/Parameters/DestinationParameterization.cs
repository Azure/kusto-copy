namespace KustoCopyConsole.Parameters
{
    public class DestinationParameterization
    {
        public string? ClusterQueryConnectionString { get; set; }

        public int ConcurrentQueryCount { get; set; } = 2;
     
        public int ConcurrentIngestionCount { get; set; } = 0;

        internal void Validate()
        {
            if (string.IsNullOrWhiteSpace(ClusterQueryConnectionString))
            {
                throw new CopyException(
                    $"{nameof(ClusterQueryConnectionString)} isn't specified");
            }
        }
    }
}