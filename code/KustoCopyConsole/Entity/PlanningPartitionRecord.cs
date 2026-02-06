using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record PlanningPartitionRecord(
        IterationKey IterationKey,
        int Generation,
        bool IsLeftExplored,
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