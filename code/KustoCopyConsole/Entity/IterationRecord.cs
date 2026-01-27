using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record IterationRecord(
        IterationState State,
        IterationKey IterationKey,
        string? CursorStart,
        string CursorEnd,
        DateTime? LastBlockEndIngestionTime)
        : RecordBase
    {
        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(IterationKey.ActivityName))
            {
                throw new InvalidDataException(
                    $"{nameof(IterationKey.ActivityName)} must have a value");
            }
            if (IterationKey.IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationKey.IterationId)} should be positive but is " +
                    $"{IterationKey.IterationId}");
            }
            if (State != IterationState.Starting && string.IsNullOrWhiteSpace(CursorEnd))
            {
                throw new InvalidDataException($"{nameof(CursorEnd)} should have a value");
            }
        }
    }
}