namespace KustoCopyConsole.Kusto.Data
{
    internal record OperationStatus(
        string OperationId,
        TimeSpan Duration,
        string State,
        string Status,
        bool ShouldRetry);
}