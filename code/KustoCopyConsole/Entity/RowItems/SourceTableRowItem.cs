using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class SourceTableRowItem : RowItemBase
    {
        public SourceTableState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public string CursorStart { get; set; } = string.Empty;

        public string CursorEnd { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
            if (IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationId)} should be positive but is {IterationId}");
            }
            if (string.IsNullOrWhiteSpace(CursorEnd))
            {
                throw new InvalidDataException($"{nameof(CursorEnd)} should have a value");
            }
        }

        public RowItemBase ChangeState(SourceTableState newState)
        {
            var clone = (SourceTableRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}