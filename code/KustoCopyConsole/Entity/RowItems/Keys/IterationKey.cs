using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems.Keys
{
    internal record IterationKey(string ActivityName, long IterationId)
    {
        public override string ToString()
        {
            return $"({ActivityName}, {IterationId})";
        }
    }
}
