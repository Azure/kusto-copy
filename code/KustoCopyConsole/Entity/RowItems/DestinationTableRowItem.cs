using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class DestinationTableRowItem : RowItemBase
    {
        public DestinationTableState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;
        
        public TableIdentity DestinationTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public string StagingTableName { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
            DestinationTable.Validate();
            if (IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationId)} should be positive but is {IterationId}");
            }
            if (string.IsNullOrWhiteSpace(StagingTableName))
            {
                throw new InvalidDataException($"{nameof(StagingTableName)} should have a value");
            }
        }

        public DestinationTableRowItem ChangeState(DestinationTableState newState)
        {
            var clone = (DestinationTableRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}