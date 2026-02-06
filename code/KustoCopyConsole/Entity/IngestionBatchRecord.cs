using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record IngestionBatchRecord(BlockKey BlockKey, string OperationText) : RecordBase
    {
        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(BlockKey.IterationKey.ActivityName))
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.IterationKey.ActivityName)} must have a value");
            }
            if (BlockKey.IterationKey.IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.IterationKey.IterationId)} should be " +
                    $"positive but is {BlockKey.IterationKey.IterationId}");
            }
            if (BlockKey.BlockId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.BlockId)} should be positive but is {BlockKey.BlockId}");
            }
            if (string.IsNullOrWhiteSpace(OperationText))
            {
                throw new InvalidDataException($"{nameof(OperationText)} should contain a value");
            }
        }
    }
}