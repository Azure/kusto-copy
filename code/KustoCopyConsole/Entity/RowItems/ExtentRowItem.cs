using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    internal class ExtentRowItem : RowItemBase
    {
        public string ActivityName { get; set; } = string.Empty;

        public long IterationId { get; set; }

        public long BlockId { get; set; }

        public string ExtentId { get; set; } = string.Empty;

        public long RowCount { get; set; }

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