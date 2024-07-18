namespace KustoCopyConsole.JobParameters
{
    public class DestinationParameterization
    {
        public Uri DestinationClusterUri { get; set; } = new Uri();

        public string DatabaseName { get; set; } = string.Empty;
    }
}