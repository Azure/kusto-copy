using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    internal class DbCommandClient : KustoClientBase
    {
        private readonly ICslAdminProvider _provider;

        public DbCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoPriority> commandQueue,
            string databaseName)
            : base(commandQueue)
        {
            _provider = provider;
            DatabaseName = databaseName;
        }

        public string DatabaseName { get; }

        public async Task<int> ShowExportCapacityAsync(
            KustoPriority priority,
            CancellationToken ct)
        {
            return await RequestRunAsync(
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
                    var result = reader
                        .ToEnumerable(r => (long)r[0])
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
                return await RequestRunAsync(
                    priority,
                    async () =>
                    {
                        var operationIdsText = string.Join(", ", operationIds);
                        var commandText = @$".show operations({operationIdsText})
| project OperationId, Duration, State, Status, ShouldRetry";
                        var reader = await _provider.ExecuteControlCommandAsync(
                            DatabaseName,
                            commandText);
                        var result = reader
                            .ToEnumerable(r => new ExportOperationStatus(
                                ((Guid)r["OperationId"]).ToString(),
                                (TimeSpan)r["Duration"],
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
            string? cursorStart,
            string cursorEnd,
            string ingestionTimeStart,
            string ingestionTimeEnd,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var rootListText = string.Join(", ", storageRootUris.Select(u => $"h'{u}'"));
                    var cursorStartFilter = cursorStart == null
                    ? string.Empty
                    : $"| where cursor_after('{cursorStart}')";
                    var commandText = @$"
.export async compressed to parquet (
    {rootListText}
)
with (
    namePrefix=""export"",
    persistDetails=true,
    parquetDatetimePrecision=""microsecond"",
    distribution=""per_node""
) <| 
let ['{tableName}'] = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at('{cursorEnd}')
    | where ingestion_time() >= todatetime('{ingestionTimeStart}')
    | where ingestion_time() <= todatetime('{ingestionTimeEnd}');
['{tableName}']
{kqlQuery}
";
                    var reader = await _provider.ExecuteControlCommandAsync(
                        DatabaseName,
                        commandText);
                    var operationId = reader
                        .ToEnumerable(r => (Guid)r[0])
                        .First();

                    return operationId.ToString();
                });
        }

        public async Task<IImmutableList<ExportDetail>> ShowExportDetailsAsync(
            KustoPriority priority,
            string operationId,
            CancellationToken ct)
        {
            return await RequestRunAsync(
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
                    var result = reader
                        .ToEnumerable(r => new ExportDetail(
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
            await RequestRunAsync(
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

                    return 0;
                });
        }

        public async Task CreateTempTableAsync(
            KustoPriority priority,
            string tableName,
            string tempTableName,
            CancellationToken ct)
        {
            await RequestRunAsync(
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
                    var result = reader
                        .ToEnumerable(r => new
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

                    return 0;
                });
        }
        #endregion

        public async Task<IImmutableList<TagRowCount>> GetIngestedTagRowCountsAsync(
            KustoPriority priority,
            IEnumerable<string> tags,
            IEnumerable<long> expectedRowCounts,
            string tempTableName,
            CancellationToken ct)
        {
            return await RequestRunAsync(
               priority,
               async () =>
               {
                   var tagListText = string.Join(",", tags.Select(t => $"'{t}'"));
                   var rowCountListText = string.Join(",", expectedRowCounts);
                   var commandText = @$"
.show table ['{tempTableName}'] extents
| summarize RowCount=sum(RowCount) by Tags
| lookup kind=inner (
    print Tags=dynamic([{tagListText}]), ExpectedRowCount=dynamic([{rowCountListText}])
    | mv-expand Tags to typeof(string), ExpectedRowCount to typeof(long)) on Tags
//  We keep only the ingested tags
| where ExpectedRowCount<=RowCount
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var result = reader
                        .ToEnumerable(r => new TagRowCount(
                            (string)r["Tags"],
                            (long)r["RowCount"]))
                        .ToImmutableArray();

                   return result;
               });
        }

        public async Task<IEnumerable<ExtentRowCount>> GetExtentRowCountsAsync(
            KustoPriority priority,
            IEnumerable<string> tags,
            string tempTableName,
            CancellationToken ct)
        {
            return await RequestRunAsync(
               priority,
               async () =>
               {
                   var tagListText = string.Join(",", tags.Select(t => $"'{t}'"));
                   var commandText = @$"
.show table ['{tempTableName}'] extents
| where Tags in ({tagListText})
| project ExtentId, Tags, RowCount
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var results = reader
                        .ToEnumerable(r => new ExtentRowCount(
                            ((Guid)r["ExtentId"]).ToString(),
                            (string)r["Tags"],
                            (long)r["RowCount"]))
                        .ToImmutableArray();

                   return results;
               });
        }

        public async Task<int> MoveExtentsAsync(
            KustoPriority priority,
            string tempTableName,
            string tableName,
            IEnumerable<string> extentIds,
            CancellationToken ct)
        {
            return await RequestRunAsync(
               priority,
               async () =>
               {
                   var extentIdTextList = string.Join(", ", extentIds);
                   var commandText = @$"
.move extents from table ['{tempTableName}'] to table ['{tableName}']
    with (setNewIngestionTime=true)
    ({extentIdTextList})
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var results = reader
                    .ToEnumerable(r => new
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
            IEnumerable<string> tags,
            CancellationToken ct)
        {
            return await RequestRunAsync(
               priority,
               async () =>
               {
                   var tagListText = string.Join(", ", tags.Select(t => $"'{t}'"));
                   var commandText = @$"
.drop table ['{tableName}'] extent tags
    ({tagListText})
";
                   var properties = new ClientRequestProperties();
                   var reader = await _provider.ExecuteControlCommandAsync(
                       DatabaseName,
                       commandText,
                       properties);
                   var results = reader
                    .ToEnumerable(r => new
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