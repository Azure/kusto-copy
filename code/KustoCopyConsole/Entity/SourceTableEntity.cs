using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Entity
{
    internal class SourceTableEntity : IterationEntityBase
    {
        public SourceTableEntity()
        {
            throw new NotImplementedException();
        }

        public SourceTableState State { get; }

        public string TableName { get; }
    }
}