using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal abstract class DestinationDatabaseEntityBase : IterationEntityBase
    {
        protected DestinationDatabaseEntityBase(DatabaseReference destinationDatabase)
        {
            DestinationDatabase = destinationDatabase;
        }

        public DatabaseReference DestinationDatabase { get; }
    }
}