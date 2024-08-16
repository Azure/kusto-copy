using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    public class DbQueryClient
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

        public async Task<string> GetCurrentCursor(CancellationToken ct)
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

        public async Task<(string IngestionTimeEnd, long Cardinality)> GetPlanningCutOffIngestionTimeAsync(
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
                    const string INGESTION_TIME_START_PARAM = "ingestionTimeStartText";

                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:string);
let IngestionTimeStart = todatetime({INGESTION_TIME_START_PARAM});
let BaseData = ['{tableName}']
    | project IngestionTime = ingestion_time()
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}))
    | where iif(isempty({INGESTION_TIME_START_PARAM}), true, IngestionTime>IngestionTimeStart);
let IngestionTimeEnd = toscalar(BaseData
    //  The +1 is to see if the last ingestion time slice goes beyond the 1M
    | top 1048576+1 by IngestionTime asc
    | summarize by IngestionTime
    //  Here we want to skip the last slice if there is more than one slice since the last slice is above 1M
    | top 2 by IngestionTime desc
    //  But we also want to keep the last slice if it's the only slice
    | summarize min(IngestionTime));
//  Simply return the cardinality and ingestion time boundary
BaseData
| where IngestionTime <= IngestionTimeEnd
| summarize Cardinality=count()
| project IngestionTimeEnd=tostring(IngestionTimeEnd), Cardinality";
                    var properties = EMPTY_PROPERTIES.Clone();

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
                        .Select(r => new
                        {
                            IngestionTimeEnd = (string)(r[0]),
                            Cardinality = (long)r[1]
                        })
                        .FirstOrDefault();

                    if (result == null)
                    {
                        return (string.Empty, 0);
                    }
                    else
                    {
                        return (result.IngestionTimeEnd, result.Cardinality);
                    }
                });
        }
    }
}