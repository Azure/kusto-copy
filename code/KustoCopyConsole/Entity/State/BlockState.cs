namespace KustoCopyConsole.Entity.State
{
    public enum BlockState
    {
        Planned,
        Exporting,
        CompletingExport,
        Exported,
        Queuing,
        Queued,
        Ingested,
        ExtentMoved
    }
}