using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record ExtentRecord(
        BlockKey BlockKey,
        string ExtentId,
        long RowCount) : RecordBase
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
                    $"{nameof(BlockKey.IterationKey.IterationId)} should be positive " +
                    $"but is {BlockKey.IterationKey.IterationId}");
            }
            if (BlockKey.BlockId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.BlockId)} should be positive but is {BlockKey.BlockId}");
            }
            if (string.IsNullOrWhiteSpace(ExtentId))
            {
                throw new InvalidDataException($"{nameof(ExtentId)} must have a value");
            }
            if (RowCount < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(RowCount)} should be positive but is {RowCount}");
            }
        }
    }
}