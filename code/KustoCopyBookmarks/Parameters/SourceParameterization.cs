namespace KustoCopyBookmarks.Parameters
{
    public class SourceParameterization
    {
        public string? ClusterQueryUri { get; set; }

        public DatabaseOverrideParameterization DatabaseOverride { get; set; } =
            new DatabaseOverrideParameterization();

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as SourceParameterization;

            return other != null
                && object.Equals(ClusterQueryUri, other.ClusterQueryUri)
                && object.Equals(DatabaseOverride, other.DatabaseOverride);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}