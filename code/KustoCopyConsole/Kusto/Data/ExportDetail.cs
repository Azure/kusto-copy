using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    public record ExportDetail(Uri BlobUri, long RecordCount, long SizeInBytes);
}