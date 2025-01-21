using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class UrlRowItem : RowItemBase
    {
        public UrlState State { get; set; }

        public string ActivityName { get; set; } = string.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public string Url { get; set; } = string.Empty;

        public long RowCount { get; set; }

        public string SerializedQueuedResult { get; set; } = string.Empty;

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
            if (!Uri.TryCreate(Url, UriKind.Absolute, out _))
            {
                throw new InvalidDataException($"{nameof(Url)} is invalid:  {Url}");
            }
            if (State >= UrlState.Queued && string.IsNullOrWhiteSpace(SerializedQueuedResult))
            {
                throw new InvalidDataException($"{nameof(SerializedQueuedResult)} should not be empty");
            }
        }

        public UrlRowItem ChangeState(UrlState newState)
        {
            var clone = (UrlRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}