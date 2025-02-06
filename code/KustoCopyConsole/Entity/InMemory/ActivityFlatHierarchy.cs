using KustoCopyConsole.Entity.RowItems;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.InMemory
{
    internal record ActivityFlatHierarchy(
        ActivityRowItem Activity,
        IterationRowItem Iteration,
        TempTableRowItem? TempTable,
        BlockRowItem Block,
        IEnumerable<UrlRowItem> Urls,
        IEnumerable<ExtentRowItem> Extents);
}