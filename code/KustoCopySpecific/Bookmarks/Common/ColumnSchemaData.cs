namespace KustoCopySpecific.Bookmarks.Common
{
    public class ColumnSchemaData
    {
        public string ColumnName { get; set; } = string.Empty;

        public string ColumnType { get; set; } = string.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as ColumnSchemaData;

            return other != null
                && ColumnName == other.ColumnName
                && ColumnType == other.ColumnType;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        #endregion
    }
}