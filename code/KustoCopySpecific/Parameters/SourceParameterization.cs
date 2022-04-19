using System.Collections.Immutable;

namespace KustoCopySpecific.Parameters
{
    public class SourceParameterization
    {
        public string? ClusterQueryUri { get; set; }

        public int ConcurrentQueryCount { get; set; } = 1;

        public int ConcurrentExportCommandCount { get; set; } = 2;

        public DatabaseConfigParameterization DatabaseDefault { get; set; } =
            new DatabaseConfigParameterization();

        public IImmutableList<SourceDatabaseParameterization> Databases { get; set; } =
            ImmutableArray<SourceDatabaseParameterization>.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as SourceParameterization;

            return other != null
                && string.Equals(ClusterQueryUri, other.ClusterQueryUri)
                && ConcurrentQueryCount == other.ConcurrentQueryCount
                && ConcurrentExportCommandCount == other.ConcurrentExportCommandCount
                && object.Equals(DatabaseDefault, other.DatabaseDefault)
                && Databases.SequenceEqual(other.Databases);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}