using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class TableColumn
    {
        public string Name { get; set; } = string.Empty;

        public string Type { get; set; } = string.Empty;

        #region Object methods
        public override bool Equals(object? obj)
        {
            var other = obj as TableColumn;

            return other != null
                && other.Name == Name
                && other.Type == Type;
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode() ^ Type.GetHashCode();
        }
        #endregion
    }
}