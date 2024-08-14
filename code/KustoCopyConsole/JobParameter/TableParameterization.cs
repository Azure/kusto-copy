
using KustoCopyConsole.Entity;

namespace KustoCopyConsole.JobParameter
{
    public class TableParameterization
    {
        public string ClusterUri { get; set; } = string.Empty;
        
        public string DatabaseName { get; set; } = string.Empty;
        
        public string TableName { get; set; } = string.Empty;

        public void Validate()
        {
            throw new NotImplementedException();
        }

        public TableIdentity GetTableIdentity()
        {
            return new TableIdentity(
                NormalizedUri.NormalizeUri(ClusterUri),
                DatabaseName,
                TableName);
        }
    }
}