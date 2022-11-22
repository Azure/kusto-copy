using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.KustoQuery
{
    public record ExportOutput(Uri Path, long RecordCount, long SizeInBytes);
}