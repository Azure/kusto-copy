namespace KustoCopyBookmarks.Parameters
{
    public class ConfigurationParameterization
    {
        public string ExportSlots { get; set; } = "%10";
        
        public int ConcurrentQueryCount { get; set; } = 10;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as ConfigurationParameterization;

            return other != null
                && object.Equals(ExportSlots, other.ExportSlots);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}