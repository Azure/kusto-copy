﻿namespace KustoCopyBookmarks.Parameters
{
    public class DestinationParameterization
    {
        public string? ClusterQueryUri { get; set; }

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as DestinationParameterization;

            return other != null
                && object.Equals(ClusterQueryUri, other.ClusterQueryUri);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}