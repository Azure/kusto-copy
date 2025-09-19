namespace KustoCopyConsole.Db.State
{
    public enum BlockState
    {
        Planned,
        Exporting,
        Exported,
        Queued,
        Ingested,
        ExtentMoved
    }
}