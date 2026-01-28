using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RecordStats(
        long RecordCount,
        string MinIngestionTime,
        string MaxIngestionTime,
        string MedianIngestionTime);
}