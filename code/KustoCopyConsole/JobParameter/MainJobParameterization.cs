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
            var sourceUriDuplicate = SourceClusters
                .Select(s => NormalizedUri.NormalizeUri(s.SourceClusterUri))
                .GroupBy(s => s)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .FirstOrDefault();

            if (sourceUriDuplicate!=null)
            {
                throw new CopyException(
                    $"Cluster URI '{sourceUriDuplicate}' appears twice in the parameterization",
                    false);
            }
            foreach (var s in SourceClusters)
            {
                s.Validate();
            }
        }

        internal string ToYaml()
        {
            var serializer = new SerializerBuilder().Build();
            var yaml = serializer.Serialize(this);

            return yaml;
        }
    }
}