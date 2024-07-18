using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public record SubIterationKey(long IterationId, long SubIterationId)
    {
        public static SubIterationKey FromSubIteration(StatusItem recordBatch)
        {
            var key = new SubIterationKey(
                recordBatch.IterationId,
                recordBatch.SubIterationId!.Value);

            return key;
        }
    }
}