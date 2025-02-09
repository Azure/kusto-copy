namespace KustoCopyConsole.Entity.RowItems.Keys
{
    internal record BlockKey(string ActivityName, long IterationId, long BlockId)
    {
        public override string ToString()
        {
            return $"('{ActivityName}', {IterationId}, {BlockId})";
        }
    }
}