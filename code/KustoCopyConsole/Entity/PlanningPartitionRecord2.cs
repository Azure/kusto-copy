using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record PlanningPartitionRecord2(
        IterationKey IterationKey,
        int Level,
        //  Partition ID is unique only within a parent partition
        //  The reason that isn't an issue is that we do depth-first, so repeating partition IDs
        //  will never be present together
        int PartitionId,
        long RowCount,
        long ExtentCount,
        string MinIngestionTime,
        string MaxIngestionTime) : RecordBase
    {
        public override void Validate()
        {
        }
    }
}