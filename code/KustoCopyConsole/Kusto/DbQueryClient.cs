﻿using Kusto.Data.Common;
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
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $"| where cursor_after('{cursorStart}')";
                    var query = @$"
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at('{cursorEnd}')
    {kqlQuery};
BaseData
| summarize
    MinIngestionTime=tostring(min(ingestion_time())),
    MaxIngestionTime=tostring(max(ingestion_time()))
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var result = reader
                        .ToEnumerable(r => new IngestionTimeInterval(
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"]))
                        .First();

                    return result;
                });
        }

        public async Task<RecordDistribution> GetRecordDistributionAsync(
            KustoPriority priority,
            string tableName,
            string? kqlQuery,
            string cursorStart,
            string cursorEnd,
            //  This is excluded
            string? lastIngestionTime,
            //  This is used to compute the upper bound
            string lowerIngestionTime,
            string upperIngestionTime,
            long maxRowCount,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {   //  Max interval is based on the string size of the extent IDs
                    //  They must all fit in a string for the .show command
                    const int MAX_EXTENT_COUNT = 25000;
                    //  Max interval we look into in one go
                    const int MAX_INTERVAL_COUNT = 500000;

                    var dbUri = $"{_queryUri.ToString().TrimEnd('/')}/{_databaseName}";
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $@"| where cursor_after(""{cursorStart}"")";
                    var lowerIngestionTimeFilter = string.IsNullOrWhiteSpace(lastIngestionTime)
                    ? string.Empty
                    : $@"| where ingestion_time()>todatetime('{lastIngestionTime}')";
                    var query = @$"
let MaxExtentCount = {MAX_EXTENT_COUNT};
let MaxIntervalCount = {MAX_INTERVAL_COUNT};
let MaxRowCount = {maxRowCount};
let TimeHorizon = 1d;
let UpperIngestionTimeFilter = todatetime('{lowerIngestionTime}')+TimeHorizon;
let BaseData = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at(""{cursorEnd}"")
    {lowerIngestionTimeFilter}
    | where ingestion_time()<=UpperIngestionTimeFilter
    {kqlQuery}
    ;
//  Fetch a batch of ingestion-time interval with extent IDs
//  Do a first pass by merging ingestion time in the same extent id (regardless of row count)
let ExtentsWithIngestionTimeIntervals = materialize(BaseData
    | summarize RowCount=count() by IngestionTime=ingestion_time(), ExtentId=extent_id()
    //  We top number of intervals so the data set is workable in memory
    | top MaxIntervalCount by IngestionTime asc
    //  We keep the interval number so that if it matches MaxIntervalCount, corresponding ingestion time end should be dropped
    | extend IntervalNumber=row_number(1)
    | extend Rank=row_rank_dense(ExtentId)
    | summarize IngestionTimeStart=min(IngestionTime), IngestionTimeEnd=max(IngestionTime),
        ExtentId=take_any(ExtentId), RowCount=sum(RowCount),
        MaxIntervalNumber=max(IntervalNumber)
        by Rank
    //  We clip the MaxIntervalCount's interval as it may contain only parts of the
    //  data pertaining to its ingestion_time
    | where MaxIntervalNumber!=MaxIntervalCount
    | project-away Rank, MaxIntervalNumber
    | extend ExtentId=tostring(ExtentId));
//  Compile the unique extent IDs with the intent of clipping them to MaxExtentCount
let ExtentIdWithIngestionTime = ExtentsWithIngestionTimeIntervals
    | summarize IngestionTimeStart=min(IngestionTimeStart) by ExtentId
    //  Clip the extent ids:  this will give us an IngestionTimeStart to clip from
    | top MaxExtentCount by IngestionTimeStart asc;
let ExtentIds = ExtentIdWithIngestionTime
    | summarize make_list(ExtentId);
//  Build a .show command to get the extent id creation time
let ShowCommand = strcat(
    "".show table ['{tableName}'] extents ("",
    strcat_array(toscalar(ExtentIds), ','),
    "") | project ExtentId, MinCreatedOn""
);
let ExtentIdCreationTime = evaluate execute_show_command(""{dbUri}"", ShowCommand)
    | extend ExtentId=tostring(ExtentId)
    | project-rename CreatedOn=MinCreatedOn;
//  Join the two information and merge intervals for creation time within the same hour
//  (while keeping row count under a threshold)
ExtentsWithIngestionTimeIntervals
//  Clip the data to align with extent IDs
| where IngestionTimeStart <= toscalar(ExtentIdWithIngestionTime | summarize max(IngestionTimeStart))
| lookup kind=leftouter ExtentIdCreationTime on ExtentId
| extend GroupCreatedOn=bin(CreatedOn, 1d)
| order by IngestionTimeStart asc, IngestionTimeEnd asc
//  Fuse the records together
| scan declare (GroupId:long = 1, RunningSum:long = 0, PrevGroup:datetime = datetime(null))
    with (
        step s: true => 
            RunningSum = iff(
                s.PrevGroup == GroupCreatedOn and s.RunningSum + RowCount <= MaxRowCount,
                s.RunningSum + RowCount,
                RowCount),
            GroupId = iff(
                s.PrevGroup == GroupCreatedOn and s.RunningSum + RowCount <= MaxRowCount,
                s.GroupId,
                s.GroupId+1),
            PrevGroup=GroupCreatedOn;
    )
| summarize
    IngestionTimeStart=tostring(min(IngestionTimeStart)),
    IngestionTimeEnd=tostring(max(IngestionTimeEnd)),
    RowCount=sum(RowCount), MinCreatedOn=min(CreatedOn), MaxCreatedOn=max(CreatedOn)
    by GroupId
| project-away GroupId;
print todatetime('{upperIngestionTime}') <=
    coalesce(
        toscalar(ExtentsWithIngestionTimeIntervals | summarize max(IngestionTimeEnd)),
        UpperIngestionTimeFilter);
";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var groups = reader
                        .ToEnumerable(r => new RecordGroup(
                            (string)r["IngestionTimeStart"],
                            (string)r["IngestionTimeEnd"],
                            (long)r["RowCount"],
                            r["MinCreatedOn"].To<DateTime>(),
                            r["MaxCreatedOn"].To<DateTime>()))
                        .ToImmutableArray();

                    reader.NextResult();

                    var hasReachedUpperIngestionTime =
                        Convert.ToBoolean(reader.ToScalar<sbyte>());

                    return new RecordDistribution(groups, hasReachedUpperIngestionTime);
                });
        }
    }
}