using KustoCopyConsole.Entity.RowItems.Keys;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    internal record ExtentRecord(
        BlockKey BlockKey,
        string ExtentId,
        long RowCount) : RecordBase
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