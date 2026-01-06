namespace KustoCopyConsole.Entity
{
    internal record BlockSummaryCount(
        long Planned,
        long Exporting,
        long Exported,
        long Queued,
        long Ingested,
        long ReadyToMove,
        long ExtentMoved);
}