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
            if (string.IsNullOrWhiteSpace(ClusterUri))
            {
                throw new CopyException($"{nameof(ClusterUri)} is required", false);
            }
            if (string.IsNullOrWhiteSpace(DatabaseName))
            {
                throw new CopyException($"{nameof(DatabaseName)} is required", false);
            }
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