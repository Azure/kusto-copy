namespace KustoCopyConsole.JobParameter
{
    public class TableOption
    {
        public TimeSpan ExtentTimeRange { get; set; } = TimeSpan.FromDays(1);

        public ExportMode ExportMode { get; set; } = ExportMode.BackFillAndNew;

        public TimeSpan IterationWait { get; set; } = TimeSpan.FromMinutes(5);

        internal void Validate()
        {
        }
    }
}