using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
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
        private readonly PriorityExecutionQueue<KustoDbPriority> _queue;
        private readonly string _databaseName;

        public DbQueryClient(
            ICslQueryProvider provider,
            PriorityExecutionQueue<KustoDbPriority> queue,
            string databaseName)
        {
            _provider = provider;
            _queue = queue;
            _databaseName = databaseName;
        }

        public async Task<string> GetCurrentCursorAsync(CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                KustoDbPriority.HighestPriority,
                async () =>
                {
                    var query = "print cursor_current()";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var cursor = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => (string)r[0])
                        .FirstOrDefault();

                    return cursor!;
                });
        }

        public async Task<bool> HasNewDataAsync(
            string tableName,
            long iterationId,
            string cursorStart,
            string cursorEnd,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";

                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string);
let BaseData = ['{tableName}']
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}));
BaseData
| take 1
| count
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => (long)(r[0]) > 0)
                        .FirstOrDefault();

                    return result == true;
                });
        }

        public async Task<IImmutableList<RecordDistribution>> GetRecordDistributionAsync(
            long iterationId,
            string tableName,
            string cursorStart,
            string cursorEnd,
            DateTime? ingestionTimeStart,
            int maxStatCount,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "IngestionTimeStart";

                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:datetime=datetime(null));
let MaxStatCount = {maxStatCount};
let BaseData = ['{tableName}']
    | project IngestionTime = ingestion_time()
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}))
    | where iif(isnull({INGESTION_TIME_START_PARAM}), true, IngestionTime>todatetime({INGESTION_TIME_START_PARAM}));
let MinIngestionTime = toscalar(BaseData
    | summarize min(IngestionTime));
let ProfileData = BaseData
    | where IngestionTime < MinIngestionTime + 1d
    | summarize RowCount=count() by IngestionTime, ExtentId=tostring(extent_id());
let MaxIngestionTime = toscalar(ProfileData
    | top MaxStatCount by IngestionTime asc
    | summarize max(IngestionTime));
//  Recompute in case we did split an ingestion time in two
ProfileData
| where IngestionTime <= MaxIngestionTime
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
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new RecordDistribution(
                            (DateTime)(r[0]),
                            (string)(r[1]),
                            (long)r[2]))
                        .ToImmutableArray();

                    return result;
                });
        }
    }
}