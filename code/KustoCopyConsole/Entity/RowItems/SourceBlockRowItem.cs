using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal record SourceBlockRowItem(
        SourceBlockState State,
        TableIdentity SourceTable,
        string IterationId,
        long BlockId,
        string IngestionTimeStart,
        string IngestionTimeEnd,
        DateTime Created)
        : RowItemBase(Created, DateTime.Now)
    {
        public override void Validate()
        {
        }
    }
}