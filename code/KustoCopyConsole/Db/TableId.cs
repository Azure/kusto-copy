using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    public record TableId(Uri ClusterUri, string DatabaseName, string TableName) : RecordBase
    {
        private static readonly Uri EMPTY_URI = new Uri("http://tempuri");

        public static TableId Empty { get; }
            = new TableId(EMPTY_URI, string.Empty, string.Empty);

        public override void Validate()
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