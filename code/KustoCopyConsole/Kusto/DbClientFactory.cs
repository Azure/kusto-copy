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
    internal class DbClientFactory : IDisposable
    {
        private readonly ProviderFactory _providerFactory;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _sourceClusterExportQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _sourceClusterQueryQueueMap;

        #region Constructor
        public static async Task<DbClientFactory> CreateAsync(
            MainJobParameterization parameterization,
            TokenCredential credentials,
            CancellationToken ct)
        {
            var providerFactory = new ProviderFactory(parameterization, credentials);
            var clusterOptionMap = parameterization.ClusterOptions
                .ToImmutableDictionary(o => NormalizedUri.NormalizeUri(o.ClusterUri));
            var sourceClusters = parameterization.Activities
                .Select(a => NormalizedUri.NormalizeUri(a.Source.ClusterUri))
                .Distinct()
                .Select(uri => new
                {
                    Uri = uri,
                    Option = clusterOptionMap.ContainsKey(uri) ? clusterOptionMap[uri] : null
                })
                .Select(o => new
                {
                    o.Uri,
                    ConcurrentExportCommandCount = o.Option?.ConcurrentExportCommandCount ?? 0,
                    ConcurrentQueryCount = o.Option?.ConcurrentQueryCount ?? 0
                });
            var countTasks = sourceClusters
                .Select(s => new
                {
                    Task = GetCountsAsync(providerFactory.GetCommandProvider(s.Uri), ct),
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
                    ? Math.Max(1, 0.1 * o.Task.Result.Query)
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
            _providerFactory = providerFactory;
            _sourceClusterExportQueueMap = sourceClusterExportQueueMap;
            _sourceClusterQueryQueueMap = sourceClusterQueryQueueMap;
        }

        private static async Task<(long Export, long Query)> GetCountsAsync(
            ICslAdminProvider provider,
            CancellationToken ct)
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

        void IDisposable.Dispose()
        {
            ((IDisposable)_providerFactory).Dispose();
        }

        public DbQueryClient GetDbQueryClient(Uri sourceUri, string database)
        {
            try
            {
                var queue = _sourceClusterQueryQueueMap[sourceUri];
                var provider = _providerFactory.GetQueryProvider(sourceUri);

                return new DbQueryClient(provider, queue, database);
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{sourceUri}'", false, ex);
            }
        }
    }
}