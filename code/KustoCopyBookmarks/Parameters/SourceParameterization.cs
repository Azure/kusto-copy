using System.Collections.Immutable;

namespace KustoCopyBookmarks.Parameters
{
    public class SourceParameterization
    {
        public string? ClusterQueryUri { get; set; }

        public IImmutableList<DatabaseOverrideParameterization> DatabaseOverrides { get; set; } =
            ImmutableArray<DatabaseOverrideParameterization>.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as SourceParameterization;

            return other != null
                && object.Equals(ClusterQueryUri, other.ClusterQueryUri)
                && object.Equals(DatabaseOverrides, other.DatabaseOverrides);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}