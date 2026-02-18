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
        //  1:  Split by 1d
        //  2:  Split by 1m
        //  3:  Split by 0.01s
        int Level,
        int PartitionId,
        long RecordCount,
        string MinIngestionTime,
        string MaxIngestionTime) : RecordBase
    {
        public override void Validate()
        {
        }
    }
}