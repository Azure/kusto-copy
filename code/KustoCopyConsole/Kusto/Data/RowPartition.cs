using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RowPartition(
        long RowCount,
        string MinIngestionTime,
        string MaxIngestionTime);
}