using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    /// <summary>
    /// Date time are exposed as text to avoid conversion in .NET losing precision.
    /// </summary>>
    /// <param name="MinIngestionTime"></param>
    /// <param name="MaxIngestionTime"></param>
    internal record IngestionTimeInterval(string MinIngestionTime, string MaxIngestionTime);
}