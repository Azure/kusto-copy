namespace KustoCopyConsole.Entity.RowItems.Keys
{
    internal record BlockKey(string ActivityName, long IterationId, long BlockId)
    {
        public IterationKey ToIterationKey()
        {
            return new IterationKey(ActivityName, IterationId);
        }

        public override string ToString()
        {
            return $"('{ActivityName}', {IterationId}, {BlockId})";
        }
    }
}