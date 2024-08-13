using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.InMemory
{
    internal record TableIdentity(
        Uri ClusterUri,
        string DatabaseName,
        string TableName);
}