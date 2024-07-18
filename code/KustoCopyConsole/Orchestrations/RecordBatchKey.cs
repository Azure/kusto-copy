using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Orchestrations
{
    public record RecordBatchKey(TableKey TableKey, long RecordBatchId)
    {
        public static RecordBatchKey FromRecordBatch(StatusItem recordBatch)
        {
            var tableKey = TableKey.FromRecordBatch(recordBatch);
            var recordKey = new RecordBatchKey(tableKey, recordBatch.RecordBatchId!.Value);

            return recordKey;
        }
    }
}