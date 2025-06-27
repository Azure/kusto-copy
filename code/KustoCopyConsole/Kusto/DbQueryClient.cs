using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;
using System.Data;

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

        public async Task<bool> HasNullIngestionTime(
            KustoPriority priority,
            string tableName,
            string? kqlQuery,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    var query = @$"
let BaseData = ['{tableName}']
{kqlQuery}
;
BaseData
| where isnull(ingestion_time())
| take 1
| count
";
                    var properties = new ClientRequestProperties();

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var result = reader
                        .ToEnumerable(r => (long)r[0])
                        .First();
                    var hasNull = result != 0;

                    return hasNull;
                });
        }

        public async Task<IngestionTimeInterval> GetIngestionTimeIntervalAsync(
            KustoPriority priority,
            string tableName,
            string? kqlQuery,
            string cursorStart,
            string cursorEnd,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";

                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $"| where cursor_after({CURSOR_START_PARAM})";
                    var query = @$"
declare query_parameters({CURSOR_START_PARAM}:string, {CURSOR_END_PARAM}:string);
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at({CURSOR_END_PARAM})
    {kqlQuery}
;
BaseData
| summarize MinIngestionTime=min(ingestion_time()), MaxIngestionTime=max(ingestion_time())
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var result = reader
                        .ToEnumerable(r => new IngestionTimeInterval(
                            r["MinIngestionTime"].To<DateTime>(),
                            r["MaxIngestionTime"].To<DateTime>()))
                        .First();

                    return result;
                });
        }

        public async Task<IImmutableList<RecordDistribution>> GetRecordDistributionAsync(
            KustoPriority priority,
            string tableName,
            string? kqlQuery,
            string cursorStart,
            string cursorEnd,
            //  This is excluded
            DateTime lowerIngestionTime,
            //  This is included
            DateTime upperIngestionTime,
            long maxRowCount,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    const string LOWER_INGESTION_TIME_PARAM = "LowerIngestionTime";
                    const string UPPER_INGESTION_TIME_PARAM = "UpperIngestionTime";
                    const int MAX_INTERVAL_COUNT = 25000;

                    var dbUri = $"{_queryUri.ToString().TrimEnd('/')}/{_databaseName}";
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $@"| where cursor_after(""{cursorStart}"")";
                    var query = @$"
declare query_parameters(
    {LOWER_INGESTION_TIME_PARAM}:datetime,
    {UPPER_INGESTION_TIME_PARAM}:datetime);
let MaxIntervalCount = {MAX_INTERVAL_COUNT};
let MaxRowCount = {maxRowCount};
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    | where ingestion_time()>todatetime({LOWER_INGESTION_TIME_PARAM})
    | where ingestion_time()<=todatetime({UPPER_INGESTION_TIME_PARAM})
    {kqlQuery}
    ;
//  Fetch a batch of ingestion-time interval with extent IDs
let ExtentsWithIngestionTimeIntervals = materialize(BaseData
    | summarize RowCount=count() by IngestionTime=ingestion_time(), ExtentId=extent_id()
    //  We top number of intervals so the list of extent ids can fit in a 1MB string
    | top {MAX_INTERVAL_COUNT} by IngestionTime asc
    //  We keep the interval number so that if it matches MaxIntervals, corresponding ingestion time end should be dropped
    | extend IntervalNumber=row_number(0)
    | extend Rank=row_rank_dense(ExtentId)
    | summarize IngestionTimeStart=min(IngestionTime), IngestionTimeEnd=max(IngestionTime),
        ExtentId=take_any(ExtentId), RowCount=sum(RowCount),
        MaxIntervalNumber=max(IntervalNumber)
        by Rank
    | project-away Rank
    | extend ExtentId=tostring(ExtentId));
//  Build a .show command to get the extent id creation time
let ExtentIds = ExtentsWithIngestionTimeIntervals
    | summarize make_list(ExtentId);
let ShowCommand = strcat(
    "".show table ['{tableName}'] extents ("",
    strcat_array(toscalar(ExtentIds), ','),
    "") | project ExtentId, MinCreatedOn""
);
let ExtentIdCreationTime = evaluate execute_show_command(""{dbUri}"", ShowCommand)
    | extend ExtentId=tostring(ExtentId)
    | project-rename CreatedOn=MinCreatedOn;
//  Join the two information and merge intervals for creation time within the same day
ExtentsWithIngestionTimeIntervals
| lookup kind=leftouter ExtentIdCreationTime on ExtentId
| extend DayCreatedOn=bin(CreatedOn, 1d)
| order by IngestionTimeStart asc, IngestionTimeEnd asc
//  Fuse the records together
| scan declare (GroupId:long = 1, RunningSum:long = 0, PrevDay:datetime = datetime(null))
    with (
        step s: true => 
            RunningSum = iff(s.PrevDay == DayCreatedOn and s.RunningSum + RowCount <= MaxRowCount, s.RunningSum + RowCount, RowCount),
            GroupId = iff(s.PrevDay == DayCreatedOn and s.RunningSum + RowCount <= MaxRowCount, s.GroupId, s.GroupId+1),
            PrevDay=DayCreatedOn;
    )
| summarize IngestionTimeStart=min(IngestionTimeStart), IngestionTimeEnd=max(IngestionTimeEnd),
    RowCount=sum(RowCount), CreatedOn=min(CreatedOn), MaxIntervalNumber=max(MaxIntervalNumber) by GroupId
| project-away GroupId
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(LOWER_INGESTION_TIME_PARAM, lowerIngestionTime);
                    properties.SetParameter(UPPER_INGESTION_TIME_PARAM, upperIngestionTime);

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var intervals = reader
                        .ToEnumerable(r => new
                        {
                            IngestionTimeStart = (DateTime)r["IngestionTimeStart"],
                            IngestionTimeEnd = (DateTime)r["IngestionTimeEnd"],
                            RowCount = (long)r["RowCount"],
                            CreatedOn = r["CreatedOn"].To<DateTime>(),
                            MaxIntervalNumber = (long)r["MaxIntervalNumber"]
                        })
                        .ToImmutableArray();

                    return intervals
                    .Where(i => i.MaxIntervalNumber < MAX_INTERVAL_COUNT - 1)
                    .Select(i => new RecordDistribution(
                        i.IngestionTimeStart,
                        i.IngestionTimeEnd,
                        i.RowCount,
                        i.CreatedOn))
                    .ToImmutableArray();
                });
        }
    }
}