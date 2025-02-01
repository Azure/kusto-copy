using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    public record ExtentRowCount(string ExtentId, string Tags, long RecordCount);
}