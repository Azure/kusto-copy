using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class SourceBlockRowItem : RowItemBase
    {
        public SourceBlockState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public string IngestionTimeStart { get; set; } = string.Empty;

        public string IngestionTimeEnd { get; set; } = string.Empty;

        public string OperationId { get; set; } = string.Empty;

        public override void Validate()
        {
        }

        public SourceBlockRowItem ChangeState(SourceBlockState newState)
        {
            var clone = (SourceBlockRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}