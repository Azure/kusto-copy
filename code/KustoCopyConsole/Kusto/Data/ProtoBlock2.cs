using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    /// <summary>Proto block.</summary>
    internal record ProtoBlock2(
        string MinIngestionTime,
        string MaxIngestionTime,
        DateTime? CreationTime,
        long RecordCount);
}