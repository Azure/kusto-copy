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

        public DateTime IngestionTimeStart { get; set; } = DateTime.MinValue;

        public DateTime IngestionTimeEnd { get; set; } = DateTime.MinValue;

        public string OperationId { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
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
            if (State == SourceBlockState.Exporting && string.IsNullOrWhiteSpace(OperationId))
            {
                throw new InvalidDataException(
                    $"{nameof(OperationId)} hasn't been populated");
            }
            if (State != SourceBlockState.Exporting && !string.IsNullOrWhiteSpace(OperationId))
            {
                throw new InvalidDataException(
                    $"{nameof(OperationId)} should be empty but is '{OperationId}'");
            }
        }

        public SourceBlockRowItem ChangeState(SourceBlockState newState)
        {
            var clone = (SourceBlockRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}