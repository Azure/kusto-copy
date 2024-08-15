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

        public async Task<(DateTime? IngestionTime, long Cardinality)> GetPlanningCutOffIngestionTimeAsync(
            long iterationId,
            string tableName,
            string cursorStart,
            string cursorEnd,
            DateTime? ingestionTimeStart,
            CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                KustoDbPriority.HighestPriority,
                async () =>
                {
                    var cursorStartFilter = string.IsNullOrWhiteSpace(cursorStart)
                    ? string.Empty
                    : $"| where cursor_after('{cursorStart}')";
                    var cursorEndFilter = string.IsNullOrWhiteSpace(cursorEnd)
                    ? string.Empty
                    : $"| where cursor_before_or_at('{cursorEnd}')";
                    var ingestionTimeStartFilter = ingestionTimeStart == null
                    ? string.Empty
                    : $"| where ingestion_time() > datetime('{ingestionTimeStart}')";
                    var query = @$"
['{tableName}']
{cursorStartFilter}
{cursorEndFilter}
{ingestionTimeStartFilter}
| project IngestionTime = ingestion_time()
| top 1048576 by IngestionTime asc
| summarize BinCount=count() by IngestionTime
| order by IngestionTime asc
| extend Cardinality=row_cumsum(BinCount)
| top 2 by IngestionTime desc
| top 1 by IngestionTime asc
| project IngestionTime, Cardinality";
                    var reader = await _provider.ExecuteQueryAsync(
                        _databaseName,
                        query,
                        EMPTY_PROPERTIES,
                        ct);
                    var result = reader.ToDataSet().Tables[0].Rows
                        .Cast<DataRow>()
                        .Select(r => new
                        {
                            IngestionTime = DbNullHelper.To<DateTime>(r[0]),
                            Cardinality = (long)r[1]
                        })
                        .FirstOrDefault();

                    if (result == null)
                    {
                        return (null, 0);
                    }
                    else
                    {
                        return (result.IngestionTime, result.Cardinality);
                    }
                });
        }
    }
}