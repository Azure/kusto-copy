namespace KustoCopyConsole.JobParameters
{
    public class DestinationParameterization
    {
        public Uri DestinationClusterUri { get; set; } = new Uri(string.Empty);

        public string DatabaseName { get; set; } = string.Empty;
    }
}