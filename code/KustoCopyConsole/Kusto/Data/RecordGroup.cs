using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RecordGroup(
        string IngestionTimeStart,
        string IngestionTimeEnd,
        long RowCount,
        DateTime? MinCreatedOn,
        DateTime? MaxCreatedOn);
}