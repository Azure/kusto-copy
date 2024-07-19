using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;

namespace KustoCopyConsole.JobParameter
{
    internal class MainJobParameterization
    {
        public IImmutableList<SourceClusterParameterization> SourceClusters { get; set; } =
            ImmutableArray<SourceClusterParameterization>.Empty;

        internal void Validate()
        {
        }

        internal string ToYaml()
        {
            var serializer = new SerializerBuilder().Build();
            var yaml = serializer.Serialize(this);

            return yaml;
        }
    }
}