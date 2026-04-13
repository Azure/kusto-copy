namespace KustoCopyConsole.Entity
{
    internal enum BlockMetric
    {
        Planned,
        Exporting,
        Exported,
        Queued,
        Ingested,
        ExtentMoving,
        ExtentMoved,
        TotalPlannedRowCount,
        MovedRowCount
    }
}