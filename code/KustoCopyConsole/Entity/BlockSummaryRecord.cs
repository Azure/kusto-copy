using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record BlockSummaryRecord(
        IterationKey IterationKey,
        BlockSummaryCount BlockSummaryCount,
        long ExportedRowCount);
}