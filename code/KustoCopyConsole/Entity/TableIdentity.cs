using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    public record TableIdentity(Uri ClusterUri, string DatabaseName, string TableName)
    {
        private static readonly Uri EMPTY_URI = new Uri("http://tempuri");

        public static TableIdentity Empty { get; }
            = new TableIdentity(EMPTY_URI, string.Empty, string.Empty);

        public void Validate()
        {
            if (ClusterUri == EMPTY_URI
                || string.IsNullOrWhiteSpace(DatabaseName)
                || string.IsNullOrWhiteSpace(TableName))
            {
                throw new InvalidDataException($"Table identity is invalid:  {this}");
            }
        }

        public override string ToString()
        {
            return $"(Cluster:'{ClusterUri}', Database:'{DatabaseName}', Table:'{TableName}')";
        }

        public string ToStringCompact()
        {
            return $"{ClusterUri}{DatabaseName}/{TableName}";
        }
    }
}