namespace KustoCopyConsole.Entity.State
{
    public enum BlockState
    {
        Planned,
        Replanning,
        Exporting,
        Exported,
        Queued,
        Ingested,
        ExtentMoving,
        ExtentMoved
    }
}