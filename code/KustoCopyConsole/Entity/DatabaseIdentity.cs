using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    public record DatabaseIdentity(Uri ClusterUri, string DatabaseName)
    {
        private static readonly Uri EMPTY_URI = new Uri("http://tempuri");

        public static DatabaseIdentity Empty { get; }
            = new DatabaseIdentity(EMPTY_URI, string.Empty);

        public virtual void Validate()
        {
            if (ClusterUri == EMPTY_URI
                || string.IsNullOrWhiteSpace(DatabaseName))
            {
                throw new InvalidDataException($"Database identity is invalid:  {this}");
            }
        }

        public override string ToString()
        {
            return $"(Cluster:'{ClusterUri}', Database:'{DatabaseName}')";
        }

        public virtual string ToStringCompact()
        {
            return $"{ClusterUri}{DatabaseName}";
        }
    }
}