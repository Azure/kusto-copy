namespace KustoCopyConsole.Kusto.Data
{
    internal record ExportOperationStatus(
        string OperationId,
        string State,
        string Status,
        bool ShouldRetry);
}