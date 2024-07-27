namespace KustoCopyConsole.Entity.State
{
    public enum DestinationBlockState
    {
        /// <summary>The name of the temp table can be established at this state.</summary>
        Started,
        /// <summary>
        /// If the system fails between creating the temp table and confirming with this
        /// state, the system can retry creating with the same table name.
        /// This way, there are no orphan temp tables.
        /// </summary>
        TempTableCreated,
        /// <summary>
        /// Reason for both <see cref="Queuing"/> and <see cref="Queued"/> is if there is a failure
        /// during queuing and the result is uncertain, we can rollback by deleting the temp table
        /// and creating a new one (new name).  This would avoid data duplication.
        /// </summary>
        Queuing,
        Queued,
        Ingested,
        ExtentMoved,
        /// <summary>At this point, the temp table should be deleted.</summary>
        Completed
    }
}