using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class SourceUrlRowItem : RowItemBase
    {
        public SourceUrlState State { get; set; }

        public TableIdentity SourceTable { get; set; } = TableIdentity.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public string Url { get; set; } = string.Empty;

        public override void Validate()
        {
            SourceTable.Validate();
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
        }

        public SourceUrlRowItem ChangeState(SourceUrlState newState)
        {
            var clone = (SourceUrlRowItem)Clone();

            clone.State = newState;

            return clone;
        }
    }
}