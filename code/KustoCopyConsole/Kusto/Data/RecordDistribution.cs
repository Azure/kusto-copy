using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Kusto.Data
{
    internal record RecordDistribution(
        IImmutableList<RecordGroup> RecordGroups,
        bool HasReachedUpperIngestionTime);
}