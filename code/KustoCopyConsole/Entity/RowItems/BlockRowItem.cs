using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class BlockRowItem : RowItemBase
    {
        public BlockState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public TableIdentity DestinationTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public DateTime IngestionTimeStart { get; set; } = DateTime.MinValue;

        public DateTime IngestionTimeEnd { get; set; } = DateTime.MinValue;

        public string OperationId { get; set; } = string.Empty;

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
            if (IngestionTimeStart == DateTime.MinValue)
            {
                throw new InvalidDataException(
                    $"{nameof(IngestionTimeStart)} hasn't been populated");
            }
            if (IngestionTimeEnd == DateTime.MinValue)
            {
                throw new InvalidDataException(
                    $"{nameof(IngestionTimeEnd)} hasn't been populated");
            }
            if (State != BlockState.Planned && string.IsNullOrWhiteSpace(OperationId))
            {
                throw new InvalidDataException($"{nameof(OperationId)} hasn't been populated");
            }
            if (State == BlockState.Planned && !string.IsNullOrWhiteSpace(OperationId))
            {
                throw new InvalidDataException(
                    $"{nameof(OperationId)} should be empty but is '{OperationId}'");
            }
            if (State == BlockState.Queued && !string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should be empty");
            }
            if (State < BlockState.Queued && string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should not be empty");
            }
        }

        public BlockRowItem ChangeState(BlockState newState)
        {
            var clone = (BlockRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}