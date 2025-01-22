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
        private readonly PriorityExecutionQueue<KustoPriority> _commandQueue;

        public DbCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoPriority> commandQueue,
            string databaseName)
        {
            _provider = provider;
            _commandQueue = commandQueue;
            DatabaseName = databaseName;
        }

        public string DatabaseName { get; }

        public async Task<int> ShowExportCapacityAsync(
            KustoPriority priority,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    var commandText = @$"
.show capacity
| where Resource == 'DataExport'
| project Total";
                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => (long)r[0])
                        .First();

                    return (int)result;
                });
        }

        public async Task<IImmutableList<ExportOperationStatus>> ShowOperationsAsync(
            KustoPriority priority,
            IEnumerable<string> operationIds,
            CancellationToken ct)
        {
            if (operationIds.Any())
            {
                return await _commandQueue.RequestRunAsync(
                    priority,
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
            KustoPriority priority,
            IEnumerable<Uri> storageRootUris,
            string tableName,
            string? kqlQuery,
            string cursorStart,
            string cursorEnd,
            DateTime ingestionTimeStart,
            DateTime ingestionTimeEnd,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "IngestionTimeStart";
                    const string INGESTION_TIME_END_PARAM = "IngestionTimeEnd";

                    var rootListText = string.Join(", ", storageRootUris.Select(u => $"h'{u}'"));
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $"| where cursor_after({CURSOR_START_PARAM})";
                    var commandText = @$"
.export async compressed to parquet (
    {rootListText}
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
let ['{tableName}'] = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at({CURSOR_END_PARAM})
    | where ingestion_time() between ({INGESTION_TIME_START_PARAM}.. {INGESTION_TIME_END_PARAM});
['{tableName}']
{kqlQuery}
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
            KustoPriority priority,
            string tableName,
            IEnumerable<string> extentIds,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                priority,
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
            KustoPriority priority,
            string operationId,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
                priority,
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

        #region Table Management
        public async Task DropTableIfExistsAsync(
            KustoPriority priority,
            string tableName,
            CancellationToken ct)
        {
            await _commandQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    var commandText = @$"
.drop table ['{tableName}'] ifexists
";
                    var properties = new ClientRequestProperties();

                    await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText,
                        properties);
                });
        }

        public async Task CreateTempTableAsync(
            KustoPriority priority,
            string tableName,
            string tempTableName,
            CancellationToken ct)
        {
            await _commandQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    var commandText = @$"
.execute database script with (ContinueOnErrors=true) <|
    .create table ['{tempTableName}'] based-on ['{tableName}'] with (folder=""kc"")

    .delete table ['{tempTableName}'] policy extent_tags_retention

    .alter table ['{tempTableName}'] policy ingestionbatching
    ```
    {{
        ""MaximumBatchingTimeSpan"" : ""00:00:15"",
        ""MaximumNumberOfItems"" : 2000,
        ""MaximumRawDataSizeMB"": 2048
    }}
    ```

    .alter-merge table ['{tempTableName}'] policy merge
    ```
    {{
        ""AllowRebuild"": false,
        ""AllowMerge"": false
    }}
    ```

    .delete table ['{tempTableName}'] policy partitioning

    .alter table ['{tempTableName}'] policy restricted_view_access true
";
                    var properties = new ClientRequestProperties();
                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText,
                        properties);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new
                        {
                            OperationId = (Guid)(r[0]),
                            CommandType = (string)(r[1]),
                            CommandText = (string)(r[2]),
                            Result = (string)(r[3]),
                            Reason = (string)(r[4])
                        })
                        .Where(o => o.Result != "Completed")
                        .FirstOrDefault();

                    if (result != null)
                    {
                        throw new CopyException(
                            $"Command failed in operation '{result.OperationId}':  "
                            + $"{result.CommandType} / '{result.CommandText}' / {result.Result} "
                            + $"'{result.Reason}'",
                            false);
                    }
                });
        }
        #endregion

        public async Task<IImmutableList<ExtentRowCount>> GetExtentRowCountsAsync(
            KustoPriority priority,
            string tempTableName,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
               priority,
               async () =>
               {
                   var commandText = @$"
.show table ['{tempTableName}'] extents
| summarize RowCount=sum(RowCount) by Tags
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var result = reader.ToDataSet().Tables[0].Rows
                       .Cast<DataRow>()
                       .Select(r => new ExtentRowCount(
                           (string)r["Tags"],
                           (long)r["RowCount"]))
                       .ToImmutableArray();

                   return result;
               });
        }

        public async Task<int> MoveExtentsAsync(
            KustoPriority priority,
            string tempTableName,
            string tableName,
            string blockTag,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
               priority,
               async () =>
               {
                   var commandText = @$"
.move extents to table {tableName} <|
    .show table ['{tempTableName}'] extents where tags contains '{blockTag}'
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var results = reader.ToDataSet().Tables[0].Rows
                       .Cast<DataRow>()
                       .Select(r => new
                       {
                           OriginalExtentId = (string)(r[0]),
                           ResultExtentId = (string)(r[1]),
                           Details = r[2].ToString()
                       })
                       .ToImmutableArray();
                   var singleDetail = results
                   .Where(r => !string.IsNullOrWhiteSpace(r.Details))
                   .Select(r => r.Details)
                   .FirstOrDefault();

                   if (singleDetail != null)
                   {
                       throw new CopyException($"Move extent failure:  '{singleDetail}'", true);
                   }

                   return results.Count();
               });
        }

        public async Task<int> CleanExtentTagsAsync(
            KustoPriority priority,
            string tableName,
            string blockTag,
            CancellationToken ct)
        {
            return await _commandQueue.RequestRunAsync(
               priority,
               async () =>
               {
                   var commandText = @$"
.drop table {tableName} extent tags <|
    .show table ['{tableName}'] extents where tags contains '{blockTag}'
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var results = reader.ToDataSet().Tables[0].Rows
                       .Cast<DataRow>()
                       .Select(r => new
                       {
                           OriginalExtentId = (string)(r[0]),
                           ResultExtentId = (string)(r[1]),
                           ResultExtentTags = (string)(r[2]),
                           Details = r[3].ToString()
                       })
                       .ToImmutableArray();
                   var singleDetail = results
                   .Where(r => !string.IsNullOrWhiteSpace(r.Details))
                   .Select(r => r.Details)
                   .FirstOrDefault();

                   if (singleDetail != null)
                   {
                       throw new CopyException($"Clean extent failure:  '{singleDetail}'", true);
                   }

                   return results.Count();
               });
        }
    }
}