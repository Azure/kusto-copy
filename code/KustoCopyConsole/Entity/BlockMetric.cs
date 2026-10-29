namespace KustoCopyConsole.Entity
{
    internal enum BlockMetric
    {
        Planned,
        Replanning,
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