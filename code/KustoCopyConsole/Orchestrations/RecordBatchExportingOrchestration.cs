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
        private readonly KustoExportQueue _exportQueue;
        private readonly DataLakeDirectoryClient _folderClient;

        #region Constructors
        public static async Task ExportAsync(
            StatusItem record,
            DatabaseStatus dbStatus,
            KustoExportQueue kustoExportQueue,
            DataLakeDirectoryClient folderClient,
            CancellationToken ct)
        {
            var orchestrator = new RecordBatchExportingOrchestration(
                record,
                dbStatus,
                kustoExportQueue,
                folderClient);

            await orchestrator.RunAsync(ct);
        }

        private RecordBatchExportingOrchestration(
            StatusItem record,
            DatabaseStatus dbStatus,
            KustoExportQueue kustoExportQueue,
            DataLakeDirectoryClient folderClient)
        {
            _priority = new KustoPriority(
                record.IterationId,
                record.SubIterationId!.Value,
                dbStatus.DbName,
                record.TableName);
            _recordBatchId = record.RecordBatchId!.Value;
            _dbStatus = dbStatus;
            _exportQueue = kustoExportQueue;
            _folderClient = folderClient;
        }
        #endregion

        private async Task RunAsync(CancellationToken ct)
        {
            var deleteFolderTask = _folderClient.DeleteIfExistsAsync(cancellationToken: ct);
            var preSchema = await FetchSchemaAsync(ct);
            var recordBatch = _dbStatus.GetRecordBatch(
                _priority.IterationId!.Value,
                _priority.SubIterationId!.Value,
                _recordBatchId);
            var planState = recordBatch.InternalState.RecordBatchState!.PlanRecordBatchState!;
            var cursorWindow = _dbStatus.GetCursorWindow(recordBatch.IterationId);

            await deleteFolderTask;

            var exportOutputs = await _exportQueue.ExportAsync(
                _priority,
                _folderClient.Uri,
                cursorWindow,
                planState.IngestionTimes,
                planState.RecordCount!.Value,
                ct);
            var postSchema = await FetchSchemaAsync(ct);

            if (preSchema.SequenceEqual(postSchema))
            {
                var newRecordBatch = recordBatch.UpdateState(StatusItemState.Exported);

                newRecordBatch.InternalState!.RecordBatchState!.ExportRecordBatchState =
                    new ExportRecordBatchState
                    {
                        TableColumns = preSchema,
                        BlobPaths = exportOutputs.Select(e => e.Path).ToImmutableList(),
                        RecordCount = exportOutputs.Sum(e => e.RecordCount)
                    };

                await _dbStatus.PersistNewItemsAsync(new[] { newRecordBatch }, ct);
            }
            else
            {   //  Retry as the schema change while exporting table
                await RunAsync(ct);
            }
        }

        private async Task<IImmutableList<TableColumn>> FetchSchemaAsync(CancellationToken ct)
        {
            var queryText = $@"
.show table ['{_priority.TableName}'] schema as json
| project Schema=todynamic(Schema)
| mv-expand Column=Schema.OrderedColumns
| project Name=tostring(Column.Name), CslType=tostring(Column.CslType)
";
            var columns = await _exportQueue.Client.ExecuteQueryAsync(
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