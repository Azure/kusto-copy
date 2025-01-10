using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class TableRowItem : RowItemBase
    {
        public TableState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public TableIdentity DestinationTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public string CursorStart { get; set; } = string.Empty;

        public string CursorEnd { get; set; } = string.Empty;

        public string TempTableName { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
            DestinationTable.Validate();
            if (IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationId)} should be positive but is {IterationId}");
            }
            if (State != TableState.Starting && string.IsNullOrWhiteSpace(CursorEnd))
            {
                throw new InvalidDataException($"{nameof(CursorEnd)} should have a value");
            }
            if (State >= TableState.TempTableCreating && string.IsNullOrWhiteSpace(TempTableName))
            {
                throw new InvalidDataException(
                    $"{nameof(TempTableName)} should have a value for" +
                    $"state {State}");
            }
        }

        public TableRowItem ChangeState(TableState newState)
        {
            var clone = (TableRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}