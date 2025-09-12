using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Db
{
    internal record ActivityRecord(
        ActivityState State,
        string ActivityName,
        TableId SourceTable,
        TableId DestinationTable) : RecordBase
    {
        public override void Validate()
        {
            if (string.IsNullOrWhiteSpace(ActivityName))
            {
                throw new InvalidDataException($"{nameof(ActivityName)} must have a value");
            }
            SourceTable.Validate();
            DestinationTable.Validate();
        }
    }
}