using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    public record TableIdentity(Uri ClusterUri, string DatabaseName, string TableName)
        : DatabaseIdentity(ClusterUri, DatabaseName)
    {
        public static new TableIdentity Empty { get; }
            = new TableIdentity(DatabaseIdentity.Empty.ClusterUri, string.Empty, string.Empty);

        public override void Validate()
        {
            base.Validate();
            if (string.IsNullOrWhiteSpace(TableName))
            {
                throw new InvalidDataException($"Table identity is invalid:  {this}");
            }
        }

        public override string ToString()
        {
            return $"(Cluster:'{ClusterUri}', Database:'{DatabaseName}', Table:'{TableName}')";
        }

        public override string ToStringCompact()
        {
            return $"{ClusterUri}{DatabaseName}/{TableName}";
        }
    }
}