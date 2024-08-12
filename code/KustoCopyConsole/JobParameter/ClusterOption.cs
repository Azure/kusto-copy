
namespace KustoCopyConsole.JobParameter
{
    public class ClusterOption
    {
        public string ClusterUri { get; set; } = string.Empty;

        public int ConcurrentQueryCount { get; set; } = 0;

        public int ConcurrentExportCommandCount { get; set; } = 0;

        public void Validate()
        {
            if (ConcurrentQueryCount < 0)
            {
                throw new CopyException(
                    $"{nameof(ConcurrentQueryCount)} should be above zero "
                    + $"but is {ConcurrentQueryCount}",
                    false);
            }
            if (ConcurrentExportCommandCount < 0)
            {
                throw new CopyException(
                    $"{nameof(ConcurrentExportCommandCount)} should be above zero "
                    + $"but is {ConcurrentExportCommandCount}",
                    false);
            }
        }
    }
}