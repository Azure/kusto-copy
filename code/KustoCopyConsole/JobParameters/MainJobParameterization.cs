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
        public IImmutableList<SourceClusterParameterization> SourceClusters { get; set; } =
            ImmutableArray<SourceClusterParameterization>.Empty;

        internal void Validate()
        {
            throw new NotImplementedException();
        }

        internal string ToYaml()
        {
            throw new NotImplementedException();
        }
    }
}