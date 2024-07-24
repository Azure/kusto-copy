using Azure.Core;
using Azure.Identity;
using CsvHelper;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.JobParameter;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;

namespace KustoCopyConsole.Kusto
{
    internal class DbClientFactory
    {
        private readonly ProviderFactory providerFactory;
        private readonly ImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterExportQueueMap;
        private readonly ImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterQueryQueueMap;

        #region Constructor
        public static async Task<DbClientFactory> CreateAsync(
            MainJobParameterization parameterization,
            TokenCredential credentials)
        {
            var providerFactory = new ProviderFactory(parameterization, credentials);
            var sourceClusters = parameterization.SourceClusters
                .Select(s => new
                {
                    Uri = NormalizedUri.NormalizeUri(s.SourceClusterUri),
                    s.ConcurrentExportCommandCount,
                    s.ConcurrentQueryCount
                });
            var countTasks = sourceClusters
                .Select(s => new
                {
                    Task = GetCounts(providerFactory.GetCommandProvider(s.Uri)),
                    Source = s
                })
                .ToImmutableArray();

            await Task.WhenAll(countTasks.Select(o => o.Task));

            var sourceClusterConfig = countTasks
                .Select(o => new
                {
                    o.Source.Uri,
                    ConcurrentExportCommandCount = o.Source.ConcurrentExportCommandCount == 0
                    ? o.Task.Result.Export
                    : o.Source.ConcurrentExportCommandCount,
                    ConcurrentQueryCount = o.Source.ConcurrentQueryCount == 0
                    ? o.Task.Result.Query
                    : o.Source.ConcurrentQueryCount
                });
            var sourceClusterExportQueueMap = sourceClusterConfig
                .Select(o => new
                {
                    o.Uri,
                    ExportQueue = new PriorityExecutionQueue<KustoDbPriority>(
                        (int)o.ConcurrentExportCommandCount)
                })
                .ToImmutableDictionary(o => o.Uri, o => o.ExportQueue);
            var sourceClusterQueryQueueMap = sourceClusterConfig
                .Select(o => new
                {
                    o.Uri,
                    QueryQueue = new PriorityExecutionQueue<KustoDbPriority>(
                        (int)o.ConcurrentQueryCount)
                })
                .ToImmutableDictionary(o => o.Uri, o => o.QueryQueue);

            return new DbClientFactory(
                providerFactory,
                sourceClusterExportQueueMap,
                sourceClusterQueryQueueMap);
        }

        private DbClientFactory(
            ProviderFactory providerFactory,
            ImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterExportQueueMap,
            ImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterQueryQueueMap)
        {
            this.providerFactory = providerFactory;
            this.sourceClusterExportQueueMap = sourceClusterExportQueueMap;
            this.sourceClusterQueryQueueMap = sourceClusterQueryQueueMap;
        }

        private static async Task<(long Export, long Query)> GetCounts(ICslAdminProvider provider)
        {
            var commandText = @"
.show capacity
| where Resource in ('DataExport', 'Queries')
| project Resource, Total";
            var reader = await provider.ExecuteControlCommandAsync(string.Empty, commandText);
            var countMap = reader.ToDataSet().Tables[0].Rows
                .Cast<DataRow>()
                .Select(r => r.ItemArray)
                .ToImmutableDictionary(a => (string)a[0]!, a => (long)a[1]!);

            return (countMap["DataExport"], countMap["Queries"]);
        }
        #endregion

    }
}