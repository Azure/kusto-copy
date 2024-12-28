using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    internal class DbCommandClient
    {
        private readonly Random _random = new();
        private readonly ICslAdminProvider _provider;
        private readonly PriorityExecutionQueue<KustoDbPriority> _commandQueue;

        public DbCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoDbPriority> commandQueue,
            string databaseName)
        {
            _provider = provider;
            _commandQueue = commandQueue;
            DatabaseName = databaseName;
        }

        public string DatabaseName { get; }

        public async Task<IImmutableList<ExportOperationStatus>> ShowOperationsAsync(
            IEnumerable<string> operationIds,
            CancellationToken ct)
        {
            if (operationIds.Any())
            {
                return await _commandQueue.RequestRunAsync(
                    KustoDbPriority.HighestPriority,
                    async () =>
                    {
                        var operationIdsText = string.Join(", ", operationIds);
                        var commandText = @$".show operations({operationIdsText})
| project OperationId, State, Status, ShouldRetry";
                        var reader = await _provider.ExecuteControlCommandAsync(
                            DatabaseName,
                            commandText);
                        var result = reader.ToDataSet().Tables[0].Rows
                            .Cast<DataRow>()
                            .Select(r => new ExportOperationStatus(
                                ((Guid)r["OperationId"]).ToString(),
                                (string)r["State"],
                                (string)r["Status"],
                                Convert.ToBoolean((SByte)r["ShouldRetry"])
                            ))
                            .ToImmutableArray();

                        return result;
                    });
            }
            else
            {
                return ImmutableArray<ExportOperationStatus>.Empty;
            }
        }

        public async Task<string> ExportBlockAsync(
            Uri storageRootUri,
            string tableName,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                KustoDbPriority.HighestPriority,
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "IngestionTimeStart";
                    const string INGESTION_TIME_END_PARAM = "IngestionTimeEnd";

                    var commandText = @$"
.export async compressed to parquet (
    h'{storageRootUri}'
)
with (
    namePrefix=""export"",
    persistDetails=true,
    parquetDatetimePrecision=""microsecond""
) <| 
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:datetime,
    {INGESTION_TIME_END_PARAM}:datetime);
let BlockData = ['{tableName}']
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}))
    | where iif(isnull({INGESTION_TIME_START_PARAM}), true, ingestion_time()>=todatetime({INGESTION_TIME_START_PARAM}))
    | where iif(isnull({INGESTION_TIME_END_PARAM}), true, ingestion_time()<=todatetime({INGESTION_TIME_END_PARAM}));
BlockData
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);
                    properties.SetParameter(INGESTION_TIME_START_PARAM, ingestionTimeStart);
                    properties.SetParameter(INGESTION_TIME_END_PARAM, ingestionTimeEnd);

                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText,
                        properties);
                    var operationId = (Guid)reader.ToDataSet().Tables[0].Rows[0][0];

                    return operationId.ToString();
                });
        }

        public async Task<IImmutableList<ExtentDate>> GetExtentDatesAsync(
            long iterationId,
            string tableName,
            IEnumerable<string> extentIds,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    var extentList = string.Join(", ", extentIds);
                    var commandText = @$"
.show table ['{tableName}'] extents ({extentList})
| project ExtentId=tostring(ExtentId), MinCreatedOn";
                    var properties = new ClientRequestProperties();
                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText,
                        properties);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new ExtentDate(
                            (string)(r[0]),
                            (DateTime)(r[1])))
                        .ToImmutableArray();

                    return result;
                });
        }

        public async Task<IImmutableList<ExportDetail>> ShowExportDetailsAsync(
            long iterationId,
            string tableName,
            string operationId,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    var commandText = @$"
.show operation {operationId} details
";
                    var properties = new ClientRequestProperties();
                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText,
                        properties);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new ExportDetail(
                            new Uri((string)(r[0])),
                            (long)(r[1]),
                            (long)(r[2])))
                        .ToImmutableArray();

                    return result;
                });
        }
    }
}