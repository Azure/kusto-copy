using Azure.Storage.Files.DataLake;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Storage;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestrations
{
    public class RecordBatchExportingOrchestration
    {
        private readonly KustoPriority _priority;
        private readonly long _recordBatchId;
        private readonly DatabaseStatus _dbStatus;
        private readonly KustoQueuedClient _sourceQueuedClient;
        private readonly DataLakeDirectoryClient _folderClient;

        #region Constructors
        public static async Task ExportAsync(
            StatusItem record,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            DataLakeDirectoryClient folderClient,
            CancellationToken ct)
        {
            var orchestrator = new RecordBatchExportingOrchestration(
                record,
                dbStatus,
                sourceQueuedClient,
                folderClient);

            await orchestrator.RunAsync(ct);
        }

        private RecordBatchExportingOrchestration(
            StatusItem record,
            DatabaseStatus dbStatus,
            KustoQueuedClient sourceQueuedClient,
            DataLakeDirectoryClient folderClient)
        {
            _priority = new KustoPriority(
                record.IterationId,
                record.SubIterationId!.Value,
                dbStatus.DbName,
                record.TableName);
            _recordBatchId = record.RecordBatchId!.Value;
            _dbStatus = dbStatus;
            _sourceQueuedClient = sourceQueuedClient;
            _folderClient = folderClient;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            var preSchema = await FetchSchemaAsync(ct);
            throw new NotImplementedException();
            //await _dbStatus.PersistNewItemsAsync(
            //    new[] { recordBatch.UpdateState(StatusItemState.Exported) },
            //    ct);
        }

        private async Task<IImmutableList<TableColumn>> FetchSchemaAsync(CancellationToken ct)
        {
            var queryText = $@"
.show table ['{_priority.TableName}'] schema as json
| project Schema=todynamic(Schema)
| mv-expand Column=Schema.OrderedColumns
| project Name=tostring(Column.Name), CslType=tostring(Column.CslType)
";
            var columns = await _sourceQueuedClient.ExecuteQueryAsync(
                _priority,
                _priority.DatabaseName!,
                queryText,
                r => new TableColumn
                {
                    Name = (string)r["Name"],
                    Type = (string)r["CslType"]
                });

            return columns;
        }
    }
}