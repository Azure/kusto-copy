using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class TempTableRowItem : RowItemBase
    {
        public TempTableState State { get; set; }

        public string ActivityName { get; set; } = string.Empty;

        public long IterationId { get; set; }

        public string TempTableName { get; set; } = string.Empty;

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
            if (string.IsNullOrWhiteSpace(TempTableName))
            {
                throw new InvalidDataException($"{nameof(TempTableName)} should have a value");
            }
        }

        public IterationKey GetIterationKey()
        {
            return new IterationKey(ActivityName, IterationId);
        }

        public TempTableRowItem ChangeState(TempTableState newState)
        {
            var clone = (TempTableRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}