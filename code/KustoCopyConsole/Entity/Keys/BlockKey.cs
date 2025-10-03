namespace KustoCopyConsole.Entity.Keys
{
    internal record BlockKey(IterationKey IterationKey, long BlockId)
    {
        public override string ToString()
        {
            return $"('{IterationKey.ActivityName}', {IterationKey.IterationId}, {BlockId})";
        }
    }
}