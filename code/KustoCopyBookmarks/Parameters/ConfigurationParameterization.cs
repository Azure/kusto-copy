namespace KustoCopyBookmarks.Parameters
{
    public class ConfigurationParameterization
    {
        public int ExportSlotsRatio { get; set; } = 10;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as ConfigurationParameterization;

            return other != null
                && object.Equals(ExportSlotsRatio, other.ExportSlotsRatio);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}