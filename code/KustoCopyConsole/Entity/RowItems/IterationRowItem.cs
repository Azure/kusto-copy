using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class IterationRowItem : RowItemBase
    {
        public IterationState State { get; set; }

        public string ActivityName { get; set; } = string.Empty;

        public long IterationId { get; set; }

        public string CursorStart { get; set; } = string.Empty;

        public string CursorEnd { get; set; } = string.Empty;

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
            if (State != IterationState.Starting && string.IsNullOrWhiteSpace(CursorEnd))
            {
                throw new InvalidDataException($"{nameof(CursorEnd)} should have a value");
            }
        }

        public IterationKey GetIterationKey()
        {
            return new IterationKey(ActivityName, IterationId);
        }

        public IterationRowItem ChangeState(IterationState newState)
        {
            var clone = (IterationRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}