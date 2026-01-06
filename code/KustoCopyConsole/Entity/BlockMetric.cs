namespace KustoCopyConsole.Entity
{
    internal enum BlockMetric
    {
        Planned,
        Exporting,
        Exported,
        Queued,
        Ingested,
        ReadyToMove,
        ExtentMoved,
        ExportedRowCount
    }
}