using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public record TableKey(long IterationId, long SubIterationId, string TableName)
    {
        public static TableKey FromRecordBatch(StatusItem recordBatch)
        {
            var tableKey = new TableKey(
                recordBatch.IterationId,
                recordBatch.SubIterationId!.Value,
                recordBatch.TableName!);

            return tableKey;
        }
    }
}