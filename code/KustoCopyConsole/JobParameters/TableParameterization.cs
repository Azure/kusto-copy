namespace KustoCopyConsole.JobParameters
{
    public class TableParameterization
    {
        public string Name { get; set; } = string.Empty;

        public TimeSpan ExtentTimeRange { get; set; } = TimeSpan.FromDays(1);

        public string Query {  get; set; } = string.Empty;
    }
}