using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.JobParameters
{
    internal class MainJobParameterization
    {
        public IImmutableList<ClusterParameterization> Clusters { get; set; } =
            ImmutableArray<ClusterParameterization>.Empty;
    }
}