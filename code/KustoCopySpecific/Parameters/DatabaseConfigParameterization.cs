using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopySpecific.Parameters
{
    public class DatabaseConfigParameterization
    {
        public bool IsEnabled { get; set; } = true;

        public long MaxRowsPerTablePerIteration { get; set; } = 10000000;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DatabaseConfigParameterization;

            return other != null
                && object.Equals(IsEnabled, other.IsEnabled)
                && object.Equals(MaxRowsPerTablePerIteration, other.MaxRowsPerTablePerIteration);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public CompleteDatabaseParameterization Override(
            MainParameterization mainParameterization,
            DatabaseOverrideParameterization? databaseOverride)
        {
            var source = mainParameterization.Source!;
            var destinations = mainParameterization.Destinations!;

            databaseOverride = databaseOverride ?? new DatabaseOverrideParameterization();

            var complete = new CompleteDatabaseParameterization
            {
                ClusterQueryUri = source.ClusterQueryUri!,
                ConcurrentQueryCount = source.ConcurrentQueryCount,
                ConcurrentExportCommandCount = source.ConcurrentExportCommandCount,
                IsEnabled = databaseOverride.IsEnabled ?? this.IsEnabled,
                MaxRowsPerTablePerIteration = databaseOverride.MaxRowsPerTablePerIteration
                ?? this.MaxRowsPerTablePerIteration,
                Destinations = destinations
            };

            return complete;
        }
        #endregion
    }
}