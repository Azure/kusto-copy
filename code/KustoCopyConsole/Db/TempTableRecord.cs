using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    internal record TempTableRecord(
        TempTableState State,
        IterationKey IterationKey,
        string TempTableName) : RecordBase
    {
        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(IterationKey.ActivityName))
            {
                throw new InvalidDataException($"{nameof(IterationKey.ActivityName)} must have a value");
            }
            if (IterationKey.IterationId < 1)
            {
                throw new InvalidDataException(
                    $"{nameof(IterationKey.IterationId)} should be " +
                    $"positive but is {IterationKey.IterationId}");
            }
            if (string.IsNullOrWhiteSpace(TempTableName))
            {
                throw new InvalidDataException($"{nameof(TempTableName)} should have a value");
            }
        }
    }
}