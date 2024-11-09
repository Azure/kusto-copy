using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal record SourceTableRowItem(
        SourceTableState State,
        TableIdentity SourceTable,
        long IterationId,
        string CursorStart,
        string CursorEnd,
        DateTime Created)
        : RowItemBase(Created, DateTime.Now)
    {
        public override void Validate()
        {
        }

        public RowItemBase ChangeState(SourceTableState newState)
        {
            return this with
            {
                State = newState,
                Updated = DateTime.Now
            };
        }
    }
}