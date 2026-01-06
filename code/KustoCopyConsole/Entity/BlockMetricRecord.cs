using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity
{
    internal record BlockMetricRecord(
        IterationKey IterationKey,
        BlockMetric BlockMetric,
        long Value);
}