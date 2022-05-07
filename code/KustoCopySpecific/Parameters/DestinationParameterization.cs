namespace KustoCopySpecific.Parameters
{
    public class DestinationParameterization
    {
        public string? ClusterQueryUri { get; set; }

        public string? ClusterIngestionUri { get; set; }

        public int ConcurrentQueryCount { get; set; } = 2;

        public bool IsEnabled { get; set; } = true;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DestinationParameterization;

            return other != null
                && object.Equals(ClusterQueryUri, other.ClusterQueryUri)
                && object.Equals(ClusterIngestionUri, other.ClusterIngestionUri)
                && object.Equals(ConcurrentQueryCount, other.ConcurrentQueryCount)
                && object.Equals(IsEnabled, other.IsEnabled);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}