using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks.Parameters
{
    public class DatabaseConfigParameterization
    {
        public bool IsEnabled { get; set; } = true;

        public int SubIterationRowCount { get; set; } = 10000000;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DatabaseConfigParameterization;

            return other != null
                && object.Equals(IsEnabled, other.IsEnabled)
                && object.Equals(SubIterationRowCount, other.SubIterationRowCount);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public DatabaseOverrideParameterization Override(
            DatabaseOverrideParameterization databaseOverride)
        {
            var combination = new DatabaseOverrideParameterization
            {
                Name = databaseOverride.Name,
                DestinationName = databaseOverride.DestinationName,
                IsEnabled = databaseOverride.IsEnabled ?? this.IsEnabled,
                SubIterationRowCount = databaseOverride.SubIterationRowCount
                ?? this.SubIterationRowCount
            };

            return combination;
        }
        #endregion
    }
}