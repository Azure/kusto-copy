using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal abstract record RowItemBase(DateTime Created, DateTime Updated)
    {
        public abstract void Validate();
    }
}