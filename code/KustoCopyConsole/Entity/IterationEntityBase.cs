namespace KustoCopyConsole.Entity
{
    internal abstract class IterationEntityBase
    {
        protected IterationEntityBase(
            long iterationId,
            DatabaseReference sourceDatabase,
            DateTime creationTime,
            DateTime lastUpdateTime)
        {
            IterationId = iterationId; 
            SourceDatabase = sourceDatabase;
            CreationTime = creationTime;
            LastUpdateTime = lastUpdateTime;
        }

        public long IterationId { get; }

        public DatabaseReference SourceDatabase { get; }

        public DateTime CreationTime { get; }

        public DateTime LastUpdateTime { get; }
    }
}