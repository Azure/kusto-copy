using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;

namespace KustoCopyConsole.Kusto
{
    internal class DbQueryClient : KustoClientBase
    {
        private static readonly ClientRequestProperties EMPTY_PROPERTIES =
            new ClientRequestProperties();
        private readonly ICslQueryProvider _provider;
        private readonly Uri _queryUri;
        private readonly string _databaseName;

        public DbQueryClient(
            ICslQueryProvider provider,
            PriorityExecutionQueue<KustoPriority> queue,
            Uri queryUri,
            string databaseName)
            : base(queue)
        {
            _provider = provider;
            _queryUri = queryUri;
            _databaseName = databaseName;
        }

        public async Task<string> GetCurrentCursorAsync(
            KustoPriority priority,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var query = "print cursor_current()";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var cursor = reader
                        .ToEnumerable(r => (string)r[0])
                        .FirstOrDefault();

                    return cursor!;
                });
        }

        public async Task<IEnumerable<RowPartition>> PartitionRowsAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string? cursorEnd,
            string? minIngestionTime,
            string? maxIngestionTime,
            TimeSpan partitionResolution,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $@"| where cursor_after(""{cursorStart}"")";
                    var lowerIngestionTimeFilter = minIngestionTime == null
                    ? string.Empty
                    : $@"| where ingestion_time()>=todatetime('{minIngestionTime}')";
                    var upperIngestionTimeFilter = maxIngestionTime == null
                    ? string.Empty
                    : $@"| where ingestion_time()<=todatetime('{maxIngestionTime}')";
                    var query = @$"
let PartitionResolution=timespan({partitionResolution});
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    {lowerIngestionTimeFilter}
    {upperIngestionTimeFilter}
    {kqlQuery}
    ;
BaseData
| summarize RowCount=count(), MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time())
    by PartitionBin=bin(ingestion_time(), PartitionResolution)
| order by MinIngestionTime asc
| extend MinIngestionTime=tostring(MinIngestionTime)
| extend MaxIngestionTime=tostring(MaxIngestionTime)
| project-away PartitionBin
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var rowPartitions = reader
                        .ToEnumerable(r => new RowPartition(
                            (long)r["RowCount"],
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"]))
                        .ToImmutableArray();

                    return rowPartitions;
                });
        }

        public async Task<IEnumerable<ProtoBlock>> GetProtoBlocksAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            TimeSpan partitionResolution,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var dbUri = $"{_queryUri.ToString().TrimEnd('/')}/{_databaseName}";
                    var cursorStartFilter = cursorStart == null
                    ? string.Empty
                    : $@"| where cursor_after(""{cursorStart}"")";
                    var query = @$"
let MinIngestionTime = datetime({minIngestionTime});
let MaxIngestionTime = datetime({maxIngestionTime});
let PartitionResolution = timespan({partitionResolution});
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    | where ingestion_time()>=MinIngestionTime
    | where ingestion_time()<=MaxIngestionTime
    {kqlQuery}
    ;
//  Let's list extents from the time window
let ExtentIdsText = strcat_array(toscalar(BaseData | summarize make_set(extent_id())), ',');
let ShowCommand = toscalar(strcat(
    "".show table ['{tableName}'] extents ("",
    //  Fake a non-existing extent ID if no extent ID are available
    iif(isempty(ExtentIdsText), tostring(new_guid()), ExtentIdsText),
    "")""));
let ExtentIdCreationTime = evaluate execute_show_command(""{dbUri}"", ShowCommand)
    | project ExtentId, CreatedOn=MaxCreatedOn;
BaseData
| summarize RowCount=count(), MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time())
    by PartitionBin=bin(ingestion_time(), PartitionResolution), ExtentId=extent_id()
| lookup kind=leftouter ExtentIdCreationTime on ExtentId
| summarize RowCount=sum(RowCount), MinIngestionTime=min(MinIngestionTime), MaxIngestionTime=max(MaxIngestionTime), CreatedOn=max(CreatedOn)
    by PartitionBin
| order by MinIngestionTime asc
| extend MinIngestionTime=tostring(MinIngestionTime)
| extend MaxIngestionTime=tostring(MaxIngestionTime)
| project-away PartitionBin
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var results = reader
                        .ToEnumerable(r => new ProtoBlock(
                            (long)r["RowCount"],
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"],
                            (DateTime?)r["CreatedOn"]))
                        .ToImmutableArray();

                    return results;
                });
        }
    }
}