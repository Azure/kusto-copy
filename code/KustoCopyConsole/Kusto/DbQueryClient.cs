using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
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
        }
    }
}