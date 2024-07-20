namespace KustoCopyConsole.Entity
{
    internal abstract class IterationEntityBase
    {
        protected IterationEntityBase(
            DateTime creationTime,
            DateTime lastUpdateTime,
            DatabaseReference sourceDatabase,
            long iterationId)
        {
            CreationTime = creationTime;
            LastUpdateTime = lastUpdateTime;
            SourceDatabase = sourceDatabase;
            IterationId = iterationId;
        }

        public DateTime CreationTime { get; }

        public DateTime LastUpdateTime { get; }

        public DatabaseReference SourceDatabase { get; }

        public long IterationId { get; }

        protected static T ParseEnum<T>(string value)
            where T : struct
        {
            try
            {
                return Enum.Parse<T>(value);
            }
            catch (Exception ex)
            {
                throw new CopyException($"Invalid state:  '{value}'", false, ex);
            }
        }
    }
}