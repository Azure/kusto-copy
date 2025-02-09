using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Kusto.Data;
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
        private readonly string _databaseName;

        public DbQueryClient(
            ICslQueryProvider provider,
            PriorityExecutionQueue<KustoPriority> queue,
            string databaseName)
        {
            _provider = provider;
            _queue = queue;
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

        public async Task<IImmutableList<RecordDistribution>> GetRecordDistributionAsync(
            KustoPriority priority,
            string tableName,
            string? kqlQuery,
            string cursorStart,
            string cursorEnd,
            DateTime? ingestionTimeStart,
            int maxStatCount,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                priority,
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "IngestionTimeStart";

                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $"| where cursor_after({CURSOR_START_PARAM})";
                    var ingestionTimeStartFilter = ingestionTimeStart==null
                    ? string.Empty
                    : $"| where ingestion_time()>todatetime({INGESTION_TIME_START_PARAM})";
                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:datetime=datetime(null));
let ['{tableName}'] = ['{tableName}']
    {cursorStartFilter}
    | where cursor_before_or_at({CURSOR_END_PARAM})
    {ingestionTimeStartFilter};
let BaseData = ['{tableName}']
{kqlQuery}
;
//  Cut the ingestion time away
let MaxIngestionTime = toscalar(
    BaseData
    | summarize by IngestionTime=ingestion_time(), ExtentId=tostring(extent_id())
    | top {maxStatCount} by IngestionTime asc
    | summarize max(IngestionTime));
//  Reuse the cut-away value in case the maxStatCount cut in the middle of a constant ingestion time sequence
BaseData
| summarize RowCount=count() by IngestionTime=ingestion_time(), ExtentId=extent_id()
| where IngestionTime <= MaxIngestionTime
| order by IngestionTime asc
| extend Rank=row_rank_dense(ExtentId)
| summarize IngestionTimeStart=min(IngestionTime), IngestionTimeEnd=max(IngestionTime),
    ExtentId=take_any(ExtentId), RowCount=sum(RowCount)
    by Rank
| project-away Rank
| extend ExtentId=iif(ExtentId==guid('00000000-0000-0000-0000-000000000000'), '', tostring(ExtentId))
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);
                    if (ingestionTimeStart != null)
                    {
                        properties.SetParameter(INGESTION_TIME_START_PARAM, ingestionTimeStart.Value);
                    }

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var result = reader
                        .ToEnumerable(r => new RecordDistribution(
                            (DateTime)(r["IngestionTimeStart"]),
                            (DateTime)(r["IngestionTimeEnd"]),
                            (string)(r["ExtentId"]),
                            (long)r["RowCount"]))
                        .ToImmutableArray();

                    return result;
                });
        }
    }
}