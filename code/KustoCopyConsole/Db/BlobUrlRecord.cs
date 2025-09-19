using KustoCopyConsole.Db.Keys;
using KustoCopyConsole.Db.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    internal record BlobUrlRecord(
        UrlState State,
        BlockKey BlockKey,
        Uri Url,
        long RowCount,
        string SerializedQueuedResult) : RecordBase
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
                    $"{nameof(BlockKey.IterationId)} should be " +
                    $"positive but is {BlockKey.IterationId}");
            }
            if (BlockKey.BlockId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(BlockKey.BlockId)} should be positive but is {BlockKey.BlockId}");
            }
            if (State == UrlState.Queued && string.IsNullOrWhiteSpace(SerializedQueuedResult))
            {
                throw new InvalidDataException($"{nameof(SerializedQueuedResult)} should not be empty");
            }
        }
    }
}