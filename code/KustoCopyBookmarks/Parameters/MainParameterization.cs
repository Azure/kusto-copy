using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Parameters
{
    public class MainParameterization
    {
        public SourceParameterization? Source { get; set; }

        public IImmutableList<DestinationParameterization> Destinations { get; set; } =
            ImmutableArray<DestinationParameterization>.Empty;

        public ConfigurationParameterization Configuration { get; set; }
            = new ConfigurationParameterization();

        public DatabaseConfigParameterization DatabaseDefault { get; set; } =
            new DatabaseConfigParameterization();

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as MainParameterization;

            return other != null
                && object.Equals(Source, other.Source)
                && Destinations.SequenceEqual(other.Destinations)
                && object.Equals(Configuration, other.Configuration)
                && object.Equals(DatabaseDefault, other.DatabaseDefault);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}