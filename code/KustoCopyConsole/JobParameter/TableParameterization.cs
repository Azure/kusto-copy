
namespace KustoCopyConsole.JobParameter
{
    public class TableParameterization
    {
        public string ClusterUri { get; set; } = string.Empty;
        
        public string DatabaseName { get; set; } = string.Empty;
        
        public string TableName { get; set; } = string.Empty;

        internal void Validate()
        {
            throw new NotImplementedException();
        }
    }
}