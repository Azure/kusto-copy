using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;

namespace KustoCopyConsole.Kusto
{
    internal class DbQueryClient
    {
        private static readonly ClientRequestProperties EMPTY_PROPERTIES =
            new ClientRequestProperties();
        private readonly ICslQueryProvider _provider;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly Uri _queryUri;
        private readonly string _databaseName;

        public DbQueryClient(
            ICslQueryProvider provider,
            PriorityExecutionQueue<KustoPriority> queue,
            Uri queryUri,
            string databaseName)
        {
            _provider = provider;
            _queue = queue;
            _queryUri = queryUri;
            _databaseName = databaseName;
        }

        public async Task<string> GetCurrentCursorAsync(
            KustoPriority priority,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
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

        public async Task<RecordStats> GetRecordStatsAsync(
            KustoPriority priority,
            string tableName,
            string? cursorStart,
            string? cursorEnd,
            DateTimeBoundary? minIngestionTime,
            DateTimeBoundary? maxIngestionTime,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $@"| where cursor_after(""{cursorStart}"")";
                    var lowerIngestionTimeFilter = minIngestionTime == null
                    ? string.Empty
                    : $@"| where ingestion_time(){minIngestionTime.GreaterThan}
todatetime('{minIngestionTime.IngestionTime}')";
                    var upperIngestionTimeFilter = maxIngestionTime == null
                    ? string.Empty
                    : $@"| where ingestion_time(){maxIngestionTime.LesserThan}
todatetime('{maxIngestionTime.IngestionTime}')";
                    var query = @$"
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    {lowerIngestionTimeFilter}
    {upperIngestionTimeFilter}
    ;
BaseData
| summarize RecordCount=count(), MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time())
| extend MedianIngestionTime=tostring((MaxIngestionTime+MinIngestionTime)/2)
| extend MinIngestionTime=tostring(MinIngestionTime)
| extend MaxIngestionTime=tostring(MaxIngestionTime)
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var stats = reader
                        .ToEnumerable(r => new RecordStats(
                            (long)r["RecordCount"],
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"],
                            (string)r["MedianIngestionTime"]))
                        .FirstOrDefault();

                    return stats ?? new RecordStats(0, string.Empty, string.Empty, string.Empty);
                });
        }

        public async Task<IEnumerable<ProtoBlock>> GetProtoBlocksAsync(
            KustoPriority priority,
            string tableName,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            int partitionCount,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
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
let PartitionCount={partitionCount};
let Delta = (MaxIngestionTime-MinIngestionTime)/PartitionCount;
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    | where ingestion_time()>=todatetime('{minIngestionTime}')
    | where ingestion_time()<=todatetime('{maxIngestionTime}')
    ;
//  We scan the data and find for each row its extent ID (if there is one)
//  and the partition it falls into
let ExtentIntervals = materialize(BaseData
    | project IngestionTime=ingestion_time(), ExtentId=extent_id()
    | extend i = range(1, PartitionCount)
    | mv-expand i to typeof(int)
    | extend StartI=i-1, EndI=i
    | extend StartIngestionTime = MinIngestionTime+(StartI*Delta)
    | extend EndIngestionTime = MinIngestionTime+(EndI*Delta)
    | where IngestionTime >= StartIngestionTime
    | where IngestionTime < EndIngestionTime
    | summarize RecordCount=count(),
        MinIngestionTime=min(IngestionTime), MaxIngestionTime=max(IngestionTime)
        by StartIngestionTime, ExtentId);
let ExtentIdsText = strcat_array(toscalar(ExtentIntervals | summarize make_list(ExtentId)), ',');
let ShowCommand = toscalar(strcat(
    "".show table ['{tableName}'] extents ("",
    //  Fake a non-existing extent ID if no extent ID are available
    iif(isempty(ExtentIdsText), tostring(new_guid()), ExtentIdsText),
    "") | project ExtentId, CreatedOn=MaxCreatedOn""));
let ExtentIdCreationTime = evaluate execute_show_command(""{dbUri}"", ShowCommand);
ExtentIntervals
//  We outer join so that if a merge happen in between, extent creation time will be null (hence detectable)
| lookup kind=leftouter ExtentIdCreationTime on ExtentId
| summarize RecordCount=sum(RecordCount), CreatedOn=max(CreatedOn),
    MinIngestionTime=min(MinIngestionTime), MaxIngestionTime=max(MaxIngestionTime)
    by StartIngestionTime
| order by MinIngestionTime asc
| project-away StartIngestionTime
| extend MinIngestionTime=tostring(MinIngestionTime)
| extend MaxIngestionTime=tostring(MaxIngestionTime)
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var results = reader
                        .ToEnumerable(r => new ProtoBlock(
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"],
                            (DateTime?)r["CreatedOn"],
                            (long)r["RecordCount"]))
                        .ToImmutableArray();

                    return results;
                });
        }
    }
}