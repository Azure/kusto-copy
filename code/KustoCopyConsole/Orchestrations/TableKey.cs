using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public record TableKey(SubIterationKey SubIterationKey, string TableName)
    {
        public static TableKey FromRecordBatch(StatusItem recordBatch)
        {
            var tableKey = new TableKey(
                SubIterationKey.FromSubIteration(recordBatch),
                recordBatch.TableName!);

            return tableKey;
        }
    }
}