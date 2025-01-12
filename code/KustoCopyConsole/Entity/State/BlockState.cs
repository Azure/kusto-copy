﻿namespace KustoCopyConsole.Entity.State
{
    public enum BlockState
    {
        Planned,
        Exporting,
        CompletingExport,
        Exported,
        Queued,
        Ingested,
        ExtentMoved
    }
}