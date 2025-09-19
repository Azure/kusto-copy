using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    public abstract record RecordBase()
    {
        public abstract void Validate();
    }
}