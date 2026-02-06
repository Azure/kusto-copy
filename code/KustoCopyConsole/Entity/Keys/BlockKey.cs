namespace KustoCopyConsole.Entity.Keys
{
    internal record BlockKey(IterationKey IterationKey, int BlockId)
    {
        public override string ToString()
        {
            return $"('{IterationKey.ActivityName}', {IterationKey.IterationId}, {BlockId})";
        }
    }
}