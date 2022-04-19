using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Parameters
{
    public class MainParameterization
    {
        public SourceParameterization? Source { get; set; }

        public IImmutableList<DestinationParameterization> Destinations { get; set; } =
            ImmutableArray<DestinationParameterization>.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as MainParameterization;

            return other != null
                && object.Equals(Source, other.Source)
                && Destinations.SequenceEqual(other.Destinations);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}