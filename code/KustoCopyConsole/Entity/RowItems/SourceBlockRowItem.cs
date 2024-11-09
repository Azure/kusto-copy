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
        long IterationId,
        long BlockId,
        string IngestionTimeStart,
        string IngestionTimeEnd,
        string OperationId)
        : RowItemBase(DateTime.Now, DateTime.Now)
    {
        public override void Validate()
        {
        }

        public SourceBlockRowItem ChangeState(SourceBlockState newState)
        {
            return this with
            {
                State = newState,
                Updated = DateTime.Now
            };
        }
    }
}