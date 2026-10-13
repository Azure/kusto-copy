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
        //  Legacy
        TotalPlannedRowCount,
        MovedRowCount
    }
}