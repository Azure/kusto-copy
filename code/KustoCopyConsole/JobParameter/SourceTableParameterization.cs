namespace KustoCopyConsole.JobParameter
{
    public class SourceTableParameterization
    {
        public string TableName { get; set; } = string.Empty;

        public TimeSpan ExtentTimeRange { get; set; } = TimeSpan.FromDays(1);

        public string Query {  get; set; } = string.Empty;
    }
}