using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Parameters
{
    public class MainParameterization
    {
        public SourceParameterization? Source { get; set; }

        public DestinationParameterization[]? Destinations { get; set; }

        public ConfigurationParameterization Configuration { get; set; }
            = new ConfigurationParameterization();

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as MainParameterization;

            return other != null
                && object.Equals(Source, other.Source)
                && SequenceEqual(Destinations, other.Destinations)
                && object.Equals(Configuration, other.Configuration);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion

        private static bool SequenceEqual<T>(IEnumerable<T>? first, IEnumerable<T>? second)
        {
            return (first == null && second == null)
                || (first != null && second != null && Enumerable.SequenceEqual(first, second));
        }
    }
}