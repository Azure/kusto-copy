using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal class DatabaseReference
    {
        public DatabaseReference(string clusterUri, string databaseName)
        {
            ClusterUri = clusterUri;
            DatabaseName = databaseName;
        }

        public string ClusterUri { get; }

        public string DatabaseName { get; }
    }
}