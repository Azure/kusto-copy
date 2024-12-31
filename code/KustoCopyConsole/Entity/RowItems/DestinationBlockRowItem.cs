using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class DestinationBlockRowItem : RowItemBase
    {
        public DestinationBlockState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public TableIdentity DestinationTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public string BlockTag { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
            DestinationTable.Validate();
            if (IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationId)} should be positive but is {IterationId}");
            }
            if (BlockId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockId)} should be positive but is {BlockId}");
            }
            if (State == DestinationBlockState.Queuing
                && !string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should be empty");
            }
            if (State != DestinationBlockState.Queuing
                && string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should not be empty");
            }
        }

        public DestinationBlockRowItem ChangeState(DestinationBlockState newState)
        {
            var clone = (DestinationBlockRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}