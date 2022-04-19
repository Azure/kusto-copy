using System.Collections.Immutable;

namespace KustoCopySpecific.Parameters
{
    public class SourceDatabaseParameterization
    {
        public string? Name { get; set; }

        public DatabaseOverrideParameterization? DatabaseOverrides { get; set; }

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as SourceDatabaseParameterization;

            return other != null
                && object.Equals(DatabaseOverrides, other.DatabaseOverrides);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}