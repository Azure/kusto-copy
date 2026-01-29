using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    /// <summary><see cref="DateTime"> boundary.</summary>
    internal record DateTimeBoundary(string IngestionTime, bool IsIncluded)
    {
        public string LesserThan => IsIncluded ? "<=" : "<";
        
        public string GreaterThan => IsIncluded ? ">=" : ">";
    }
}