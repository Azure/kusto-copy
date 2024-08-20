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
            TimeSpan timeResolution,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                new KustoDbPriority(iterationId, tableName),
                async () =>
                {
                    const string CURSOR_START_PARAM = "CursorStart";
                    const string CURSOR_END_PARAM = "CursorEnd";
                    const string INGESTION_TIME_START_PARAM = "ingestionTimeStartText";
                    const string TIME_RESOLUTION_PARAM = "timeResolution";

                    var query = @$"
declare query_parameters(
    {CURSOR_START_PARAM}:string,
    {CURSOR_END_PARAM}:string,
    {INGESTION_TIME_START_PARAM}:string,
    {TIME_RESOLUTION_PARAM}:timespan);
let IngestionTimeStart = todatetime({INGESTION_TIME_START_PARAM});
let BaseData = ['{tableName}']
    | project IngestionTime = ingestion_time()
    | where iif(isempty({CURSOR_START_PARAM}), true, cursor_after({CURSOR_START_PARAM}))
    | where iif(isempty({CURSOR_END_PARAM}), true, cursor_before_or_at({CURSOR_END_PARAM}))
    | where iif(isempty({INGESTION_TIME_START_PARAM}), true, IngestionTime>IngestionTimeStart);
BaseData
| summarize Cardinality=count() by IngestionTimeStart=bin(IngestionTime, {TIME_RESOLUTION_PARAM})
| top 1000 by IngestionTimeStart asc
| project
    tostring(IngestionTimeStart),
    IngestionTimeEnd=tostring(IngestionTimeStart+{TIME_RESOLUTION_PARAM}),
    Cardinality";
                    var properties = EMPTY_PROPERTIES.Clone();

                    properties.SetParameter(CURSOR_START_PARAM, cursorStart);
                    properties.SetParameter(CURSOR_END_PARAM, cursorEnd);
                    properties.SetParameter(INGESTION_TIME_START_PARAM, ingestionTimeStart);
                    properties.SetParameter(TIME_RESOLUTION_PARAM, timeResolution);

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