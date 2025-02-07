namespace KustoCopyConsole.Kusto.Data
{
    internal record ExportOperationStatus(
        string OperationId,
        TimeSpan Duration,
        string State,
        string Status,
        bool ShouldRetry);
}