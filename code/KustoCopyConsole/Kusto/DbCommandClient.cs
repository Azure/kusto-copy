using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;
using static Kusto.Data.Common.CslCommandGenerator;
using System.Text;
using static Kusto.Data.Net.Http.OneApiError;
using System;

namespace KustoCopyConsole.Kusto
{
    internal class DbCommandClient
    {
        private readonly Random _random = new();
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
            IImmutableList<Uri> storageRoots,
            CancellationToken ct)
        {
            var shuffledStorageRoots = storageRoots
                .OrderBy(i => _random.Next());
            var quotedRoots = shuffledStorageRoots
                .Select(r => @$"h""{r}""");
            var rootsText = string.Join(", ", quotedRoots);
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
                            var commandText = @$"
.export async compressed to parquet (
    {rootsText}
)
with (
    namePrefix=""export"",
    persistDetails=true
) <| 
Logs | where id == ""1234""";
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