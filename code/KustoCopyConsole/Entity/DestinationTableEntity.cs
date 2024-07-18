using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class DestinationTableEntity : DestinationDatabaseEntityBase
    {
        public DestinationTableEntity()
        {
            throw new NotImplementedException();
        }

        public DestinationTableState State { get; }

        public string TableName { get; }
    }
}