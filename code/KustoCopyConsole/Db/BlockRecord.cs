using KustoCopyConsole.Db.Keys;
using KustoCopyConsole.Db.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    internal record BlockRecord(
        BlockState State,
        BlockKey BlockKey,
        string IngestionTimeStart,
        string IngestionTimeEnd,
        DateTime MinCreationTime,
        DateTime MaxCreationTime,
        long PlannedRowCount,
        long ExportedRowCount,
        string ExportOperationId,
        string BlockTag) : RecordBase
    {
        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(BlockKey.ActivityName))
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.ActivityName)} must have a value");
            }
            if (BlockKey.IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.IterationId)} should be positive " +
                    $"but is {BlockKey.IterationId}");
            }
            if (BlockKey.BlockId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.BlockId)} should be positive but is {BlockKey.BlockId}");
            }
            if (string.IsNullOrWhiteSpace(IngestionTimeStart))
            {
                throw new InvalidDataException(
                    $"{nameof(IngestionTimeStart)} hasn't been populated");
            }
            if (string.IsNullOrWhiteSpace(IngestionTimeEnd))
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
        }
    }
}