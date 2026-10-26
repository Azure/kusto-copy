using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
using System;
using System.Collections.Immutable;

namespace KustoCopyConsole.Kusto
{
    internal class DbQueryClient : KustoClientBase
    {
        private static readonly ClientRequestProperties EMPTY_PROPERTIES = new();
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
                },
                ct);
        }

        public async Task<IEnumerable<TimeInterval>> InitialPartitionRowsAsync(
            KustoPriority priority,
            string tableName,
            string? cursorStart,
            string cursorEnd,
            TimeSpan partitionResolution,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        string.Empty,
                        string.Empty,
                        string.Empty);
                    var query = @$"
                let PartitionResolution=timespan({partitionResolution});
                {baseDataQuery}
                BaseData
                | summarize by PartitionBin=bin(ingestion_time(), PartitionResolution)
                | order by PartitionBin asc
                | project
                    MinIngestionTime=tostring(PartitionBin),
                    MaxIngestionTime=tostring(PartitionBin+PartitionResolution)
                ";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var rowPartitions = reader
                        .ToEnumerable(r => new TimeInterval(
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"]))
                        .ToImmutableArray();

                    return rowPartitions;
                },
                ct);
        }

        public async Task<IEnumerable<TimeInterval>> SubPartitionRowsAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string? minIngestionTime,
            string? maxIngestionTime,
            TimeSpan partitionResolution,
            long MaxRowCount,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        minIngestionTime,
                        maxIngestionTime,
                        kqlQuery);
                    var query = @$"
                let PartitionResolution=timespan({partitionResolution});
                {baseDataQuery}
                BaseData
                | summarize by PartitionBin=bin(ingestion_time(), PartitionResolution)
                | order by PartitionBin asc
                | project
                    MinIngestionTime=tostring(PartitionBin),
                    MaxIngestionTime=tostring(PartitionBin+PartitionResolution)
                ";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var rowPartitions = reader
                        .ToEnumerable(r => new TimeInterval(
                            (string)r["MinIngestionTime"],
                            (string)r["MaxIngestionTime"]))
                        .ToImmutableArray();

                    return rowPartitions;
                },
                ct);
        }

        public async Task<IEnumerable<RowPartition>> LoadPartitionAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            TimeSpan partitionResolution,
            long MaxRowCount,
            long MaxExtentCount,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        minIngestionTime,
                        maxIngestionTime,
                        kqlQuery);
                    var query = @$"
                let PartitionResolution=timespan({partitionResolution});
                let MaxRowCount = {MaxRowCount};
                let MaxExtentCount = {MaxExtentCount};
                {baseDataQuery}
                BaseData
                | summarize
                    RowCount=count(),
                    ExtentCount=count_distinct(extent_id()),
                    MinIngestionTime=min(ingestion_time()),
                    MaxIngestionTime=max(ingestion_time())
                    by PartitionBin=bin(ingestion_time(), PartitionResolution)
                | order by MinIngestionTime asc
                | scan declare (
                    PartitionId:long = 0,
                    RunningRowCount:long = 0,
                    RunningExtentCount:long = 0
                ) with (
                    step s: true =>
                        PartitionId = s.PartitionId + tolong(
                            s.RunningRowCount + RowCount >= MaxRowCount
                            or s.RunningExtentCount + ExtentCount >= MaxExtentCount
                        ),
                        RunningRowCount = iff(
                            s.RunningRowCount + RowCount >= MaxRowCount
                            or s.RunningExtentCount + ExtentCount >= MaxExtentCount,
                            RowCount,
                            s.RunningRowCount + RowCount
                        ),
                        RunningExtentCount = iff(
                            s.RunningRowCount + RowCount >= MaxRowCount
                            or s.RunningExtentCount + ExtentCount >= MaxExtentCount,
                            ExtentCount,
                            s.RunningExtentCount + ExtentCount
                        );
                )
                | summarize
                    RowCount = sum(RowCount),
                    ExtentCount = sum(ExtentCount),
                    MinIngestionTime = min(MinIngestionTime),
                    MaxIngestionTime = max(MaxIngestionTime)
                    by PartitionId
                | project-away PartitionId
                | order by MinIngestionTime asc
                | extend MinIngestionTime=tostring(MinIngestionTime)
                | extend MaxIngestionTime=tostring(MaxIngestionTime)
                ";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var rowPartitions = reader
                        .ToEnumerable(r => new RowPartition(
                            (long)r["RowCount"],
                            (long)r["ExtentCount"],
                            new TimeInterval(
                                (string)r["MinIngestionTime"],
                                (string)r["MaxIngestionTime"])))
                        .ToImmutableArray();

                    return rowPartitions;
                },
                ct);
        }

        public async Task<IEnumerable<string>> GetExtentIdsAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildTimeRangeBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        minIngestionTime,
                        maxIngestionTime,
                        kqlQuery);
                    var query = @$"
                {baseDataQuery}
                //  Let's list extents from the time window
                BaseData
                | summarize by ExtentId=extent_id()
                | where ExtentId != guid(00000000-0000-0000-0000-000000000000)
                ";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var results = reader
                        .ToEnumerable(r => ((Guid)r[0]).ToString())
                        .ToImmutableArray();

                    return results;
                },
                ct);
        }

        public async Task<long> GetExtentCountAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildTimeRangeBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        minIngestionTime,
                        maxIngestionTime,
                        kqlQuery);
                    var query = @$"
                {baseDataQuery}
                //  Let's list extents from the time window
                BaseData
                | summarize count_distinct(extent_id())
                ";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var results = reader
                        .ToEnumerable(r => (long)r[0])
                        .First();

                    return results;
                },
                ct);
        }

        public async Task<IEnumerable<ProtoBlock>> GetProtoBlocksAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            IEnumerable<ExtentCreationTime> extentCreationTimes,
            TimeSpan partitionResolution,
            long maxRowCountPerBlock,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                                async () =>
                                {
                                    var extentCreationTimesJsonList = string.Join(
                                        ",\n",
                                        extentCreationTimes
                                            .Select(ect =>
                                                "{ \"extentId\" : \""
                                                + ect.ExtentId
                                                + "\", \"creationTime\" : \""
                                                + ect.CreationTime
                                                + "\" }"));
                                    var baseDataQuery = BuildTimeRangeBaseDataQuery(
                                        tableName,
                                        cursorStart,
                                        cursorEnd,
                                        minIngestionTime,
                                        maxIngestionTime,
                                        kqlQuery);
                                    var query = @$"
                                    let ExtentIdCreationTime = print ExtentIdCreationTime=dynamic([
                                        {extentCreationTimesJsonList}
                                    ])
                                        | mv-expand ExtentIdCreationTime
                                        | project
                                            ExtentId=toguid(ExtentIdCreationTime.extentId),
                                            CreatedOn=todatetime(
                                                ExtentIdCreationTime.creationTime);
                                    let PartitionResolution = timespan({partitionResolution});
                                    let MaxRowCountPerBlock = long({maxRowCountPerBlock});
                                    {baseDataQuery}
                                    //  Get the data by extent
                                    let DataByExtent = BaseData
                                        | summarize
                                            RowCount=count(),
                                            MinIngestionTime=min(ingestion_time()),
                                            MaxIngestionTime=max(ingestion_time())
                                            by
                                                PartitionBin=bin(
                                                    ingestion_time(), PartitionResolution),
                                                ExtentId=extent_id()
                                        | lookup kind=leftouter ExtentIdCreationTime on ExtentId
                                        | summarize
                                            RowCount=sum(RowCount), CreatedOn=max(CreatedOn),
                                            MinIngestionTime=min(MinIngestionTime),
                                            MaxIngestionTime=max(MaxIngestionTime)
                                            by PartitionBin
                    | order by MinIngestionTime asc
                    | extend MinIngestionTime=tostring(MinIngestionTime)
                    | extend MaxIngestionTime=tostring(MaxIngestionTime)
                    | project-away PartitionBin;
                //  Merge the data into MaxRowCountPerBlock blocks
                DataByExtent
                | extend CummulativeSum = row_cumsum(RowCount)
                | extend BlockId = (CummulativeSum - 1) / MaxRowCountPerBlock
                | extend RowNum = row_number()
                | summarize
                    RowCount = sum(RowCount),
                    CreatedOn = max(CreatedOn),
                    Info1 = arg_min(RowNum, MinIngestionTime),
                    Info2 = arg_max(RowNum, MaxIngestionTime)
                  by BlockId
                | project RowCount, MinIngestionTime, MaxIngestionTime, CreatedOn";
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
                                            r["CreatedOn"].To<DateTime>()))
                                        .ToImmutableArray();

                                    return results;
                                },
                ct);
        }

        public async Task<long?> GetPostPlanningRowCountAsync(
            KustoPriority priority,
            string tableName,
            string kqlQuery,
            string postPlanningQuery,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            TimeSpan timeout,
            CancellationToken ct)
        {
            return await RequestRunAsync(
                priority,
                async () =>
                {
                    var baseDataQuery = BuildBaseDataQuery(
                        tableName,
                        cursorStart,
                        cursorEnd,
                        minIngestionTime,
                        maxIngestionTime,
                        kqlQuery);
                    var query = @$"
{baseDataQuery}
BaseData
{postPlanningQuery}
| count
";
                    try
                    {
                        var readerTask = _provider.ExecuteQueryAsync(
                            _databaseName,
                            query,
                            EMPTY_PROPERTIES,
                            ct);
                        var delayTask = Task.Delay(timeout);

                        await Task.WhenAny(readerTask, delayTask);

                        if (delayTask.IsCompleted)
                        {
                            try
                            {   //  Complete the query not to burden the cluster with
                                //  multiple concurrent queries
                                await readerTask;
                            }
                            catch { }

                            return null;
                        }
                        else
                        {
                            var reader = await readerTask;
                            var rowPartitions = reader
                                .ToEnumerable(r => (long)r[0])
                                .First();

                            return rowPartitions;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (PlanningQuery.IsResourceFailure(ex))
                        {
                            return (long?)null;
                        }
                        else
                        {
                            throw;
                        }
                    }
                },
                ct);
        }

        #region Query Builders
        private static string BuildCursorStartFilter(string? cursorStart)
        {
            return string.IsNullOrWhiteSpace(cursorStart)
                ? string.Empty
                : $@"| where cursor_after(""{cursorStart}"")";
        }

        private string BuildBaseDataQuery(
            string tableName,
            string? cursorStart,
            string cursorEnd,
            string? minIngestionTime,
            string? maxIngestionTime,
            string kqlQuery)
        {
            var lowerIngestionTimeFilter = string.IsNullOrWhiteSpace(minIngestionTime)
                ? string.Empty
                : $@"| where ingestion_time()>=todatetime('{minIngestionTime}')";
            var upperIngestionTimeFilter = string.IsNullOrWhiteSpace(maxIngestionTime)
                ? string.Empty
                : $@"| where ingestion_time()<=todatetime('{maxIngestionTime}')";
            var normalizedCursorEnd = cursorEnd ?? string.Empty;

            return $@"
let BaseData = ['{tableName}']
    {BuildCursorStartFilter(cursorStart)}
    | where cursor_before_or_at(""{normalizedCursorEnd}"")
    {lowerIngestionTimeFilter}
    {upperIngestionTimeFilter}
    {kqlQuery}
    ;";
        }

        private string BuildTimeRangeBaseDataQuery(
            string tableName,
            string? cursorStart,
            string cursorEnd,
            string minIngestionTime,
            string maxIngestionTime,
            string kqlQuery)
        {
            var normalizedCursorEnd = cursorEnd ?? string.Empty;

            return $@"
let MinIngestionTime = datetime({minIngestionTime});
let MaxIngestionTime = datetime({maxIngestionTime});
let BaseData = ['{tableName}']
    {BuildCursorStartFilter(cursorStart)}
    | where cursor_before_or_at(""{normalizedCursorEnd}"")
    | where ingestion_time()>=MinIngestionTime
    | where ingestion_time()<=MaxIngestionTime
    {kqlQuery}
    ;";
        }
        #endregion
    }
}