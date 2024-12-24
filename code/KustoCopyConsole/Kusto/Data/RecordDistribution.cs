using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RecordDistribution(
        string IngestionTime,
        string ExtentId,
        long RowCount);
}