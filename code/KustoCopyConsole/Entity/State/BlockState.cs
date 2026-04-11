namespace KustoCopyConsole.Entity.State
{
    public enum BlockState
    {
        Planned,
        Exporting,
        Exported,
        Queued,
        Ingested,
        ExtentMoving,
        ExtentMoved
    }
}