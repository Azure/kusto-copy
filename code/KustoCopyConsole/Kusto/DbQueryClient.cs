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

        public async Task<IImmutableList<RecordDistribution>> GetRecordDistributionAsync(
            long iterationId,
            string tableName,
            string cursorStart,
            string cursorEnd,
            string ingestionTimeStart,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "IngestionTimeStartText";

                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:string);
let MaxRowCount = 16000000;
let MaxStatCount = 500000;
let BaseData = ['{tableName}']
    | project IngestionTime = ingestion_time()
    | where iif(isempty(CursorStart), true, cursor_after(CursorStart))
    | where iif(isempty(CursorEnd), true, cursor_before_or_at(CursorEnd))
    | where iif(isempty(IngestionTimeStartText), true, IngestionTime>todatetime(IngestionTimeStartText));
let MinIngestionTime = toscalar(BaseData
    | summarize min(IngestionTime));
let ProfileData = BaseData
    | where IngestionTime < MinIngestionTime + 1d
    | summarize RowCount=count() by IngestionTime, ExtentId=tostring(extent_id());
let MaxIngestionTime = toscalar(ProfileData
    | top MaxStatCount by IngestionTime asc
    | extend RowCountCummulativeSum=row_cumsum(RowCount)
    | where RowCountCummulativeSum <= MaxRowCount
    | summarize max(IngestionTime));
//  Recompute in case we did split an ingestion time in two
ProfileData
| where IngestionTime <= MaxIngestionTime
| extend IngestionTime=tostring(IngestionTime)
";
                    var properties = new ClientRequestProperties();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);
                    properties.SetParameter(INGESTION_TIME_START_PARAM, ingestionTimeStart);

                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        properties,
                        ct);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new RecordDistribution(
                            (string)(r[0]),
                            (string)(r[1]),
                            (long)r[2]))
                        .ToImmutableArray();

                    return result;
                });
        }
    }
}