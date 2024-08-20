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
        /// <summary>
        /// Propagates the storage URLs.  This property isn't serialized not to appear
        /// in the serialized format nor does it make sense to take it as input from
        /// a YAML file since it should be located on the primary storage URL.
        /// </summary>
        [YamlIgnore]
        public IImmutableList<string> StorageUrls { get; set; } =
            ImmutableArray<string>.Empty;

        public bool IsContinuousRun { get; set; } = false;

        public IImmutableList<ActivityParameterization> Activities { get; set; } =
            ImmutableArray<ActivityParameterization>.Empty;

        public IImmutableList<ClusterOption> ClusterOptions { get; set; } =
            ImmutableArray<ClusterOption>.Empty;

        internal void Validate()
        {
            foreach (var a in Activities)
            {
                a.Validate();
            }
            foreach (var c in ClusterOptions)
            {
                c.Validate();
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