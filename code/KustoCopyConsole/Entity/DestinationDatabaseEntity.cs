using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class DestinationDatabaseEntity : DestinationDatabaseEntityBase
    {
        public DestinationDatabaseState State { get; }
    }
}