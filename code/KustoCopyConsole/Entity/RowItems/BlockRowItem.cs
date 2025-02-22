using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class BlockRowItem : RowItemBase
    {
        public BlockState State { get; set; }

        public string ActivityName { get; set; } = string.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public DateTime IngestionTimeStart { get; set; } = DateTime.MinValue;

        public DateTime IngestionTimeEnd { get; set; } = DateTime.MinValue;

        public DateTime MinCreationTime { get; set; } = DateTime.MinValue;

        public DateTime MaxCreationTime { get; set; } = DateTime.MinValue;

        public long PlannedRowCount { get; set; } = 0;

        public long ExportedRowCount { get; set; } = 0;

        public string ExportOperationId { get; set; } = string.Empty;

        public TimeSpan? ExportDuration { get; set; }

        public string BlockTag { get; set; } = string.Empty;

        public IImmutableList<long> ReplannedBlockIds { get; set; } = ImmutableArray<long>.Empty;

        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(ActivityName))
            {
                throw new InvalidDataException($"{nameof(ActivityName)} must have a value");
            }
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
            if (MinCreationTime == DateTime.MinValue)
            {
                throw new InvalidDataException(
                    $"{nameof(MinCreationTime)} hasn't been populated");
            }
            if (MaxCreationTime == DateTime.MinValue)
            {
                throw new InvalidDataException(
                    $"{nameof(MinCreationTime)} hasn't been populated");
            }
            if (PlannedRowCount < 0)
            {
                throw new InvalidDataException(
                    $"{nameof(PlannedRowCount)} should be superior to zero");
            }
            if (State == BlockState.Exporting && string.IsNullOrWhiteSpace(ExportOperationId))
            {
                throw new InvalidDataException(
                    $"{nameof(ExportOperationId)} hasn't been populated");
            }
            if (State != BlockState.Exporting && !string.IsNullOrWhiteSpace(ExportOperationId))
            {
                throw new InvalidDataException(
                    $"{nameof(ExportOperationId)} should be empty but is '{ExportOperationId}'");
            }
            if (State != BlockState.Queued
                && State != BlockState.Ingested
                && !string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should be empty");
            }
            if ((State == BlockState.Queued || State == BlockState.Ingested)
                && string.IsNullOrWhiteSpace(BlockTag))
            {
                throw new InvalidDataException($"{nameof(BlockTag)} should not be empty");
            }
            if (State != BlockState.Exporting && ReplannedBlockIds.Any())
            {
                throw new InvalidDataException(
                    $"{nameof(ReplannedBlockIds)} should be empty with state '{State}'");
            }
        }

        public BlockKey GetBlockKey()
        {
            return new BlockKey(ActivityName, IterationId, BlockId);
        }

        public BlockRowItem ChangeState(BlockState newState)
        {
            var clone = (BlockRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}