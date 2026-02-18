using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record PlanningPartitionRecord(
        IterationKey IterationKey,
        int Level,
        int PartitionId,
        long RecordCount,
        string MinIngestionTime,
        string MedianIngestionTime,
        string MaxIngestionTime) : RecordBase
    {
        public override void Validate()
        {
        }
    }
}