namespace KustoCopyBookmarks.ExportStorage
{
    public class TableStorageFolderData
    {
        public string FolderName { get; set; } = string.Empty;

        public DateTime OverrideIngestionTime { get; set; } = DateTime.MinValue;

        public long RowCount { get; set; } = 0;
    }
}