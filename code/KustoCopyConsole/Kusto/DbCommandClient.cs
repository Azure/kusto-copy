using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    internal class DbCommandClient
    {
        private readonly ICslAdminProvider _provider;
        private readonly PriorityExecutionQueue<KustoDbPriority> _commandQueue;
        private readonly PriorityExecutionQueue<KustoDbPriority> _exportQueue;
        private readonly string _databaseName;

        public DbCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoDbPriority> commandQueue,
            PriorityExecutionQueue<KustoDbPriority> exportQueue,
            string databaseName)
        {
            _provider = provider;
            _commandQueue = commandQueue;
            _exportQueue = exportQueue;
            _databaseName = databaseName;
        }

        public async Task<string> ExportBlockAsync(
            CancellationToken ct)
        {
            var priority = KustoDbPriority.HighestPriority;

            //  Double queue:
            //  First we wait for the export capacity
            //  Second we wait for the command processing capacity
            return await _exportQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    return await _commandQueue.RequestRunAsync(
                        priority,
                        async () =>
                        {
                            var commandText = ".export";
                            var reader = await _provider.ExecuteControlCommandAsync(
                                string.Empty,
                                commandText);
                            var operationId = (string)reader.ToDataSet().Tables[0].Rows[0][0];

                            return operationId;
                        });
                });
        }
    }
}