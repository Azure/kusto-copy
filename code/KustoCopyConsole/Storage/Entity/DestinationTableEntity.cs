using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Storage.Entity.State;

namespace KustoCopyConsole.Storage.Entity
{
    internal class DestinationTableEntity : DestinationDatabaseEntityBase
    {
        public DestinationTableState State { get; }

        public string TableName { get; }
    }
}