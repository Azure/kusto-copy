using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RecordDistribution(
        DateTime IngestionTimeStart,
        DateTime IngestionTimeEnd,
        long RowCount,
        DateTime? CreatedOn);
}