namespace KustoCopyConsole.Storage.Entity
{
    internal abstract class IterationEntityBase
    {
        public long IterationId { get; }

        public DatabaseReference SourceDatabase { get; }

        public DateTime CreationTime { get; }
        
        public DateTime LastUpdateTime { get; }
    }
}