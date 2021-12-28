namespace KustoCopyBookmarks.Parameters
{
    public class SourceParameterization
    {
        public string? ClusterQueryUri { get; set; }

        public DatabaseConfigParameterization DatabaseConfig { get; set; } =
            new DatabaseConfigParameterization();

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as SourceParameterization;

            return other != null
                && object.Equals(ClusterQueryUri, other.ClusterQueryUri)
                && object.Equals(DatabaseConfig, other.DatabaseConfig);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}