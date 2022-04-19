using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Parameters
{
    public class DatabaseOverrideParameterization
    {
        public bool? IsEnabled { get; set; } = null;

        public long? MaxRowsPerTablePerIteration { get; set; } = null;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DatabaseOverrideParameterization;

            return other != null
                && object.Equals(IsEnabled, other.IsEnabled)
                && object.Equals(MaxRowsPerTablePerIteration, other.MaxRowsPerTablePerIteration);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}