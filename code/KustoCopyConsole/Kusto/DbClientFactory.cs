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
        private const int MAX_CONCURRENT_DM_COMMAND = 2;
        private const int MAX_CONCURRENT_DB_COMMAND = 5;

        private readonly ProviderFactory _providerFactory;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _sourceClusterQueryQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _sourceClusterCommandQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _destinationClusterDmCommandQueueMap;

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
            var queryCapacityTasks = sourceClusters
                .Select(s => new
                {
                    Task = GetQueryCapacityAsync(providerFactory.GetCommandProvider(s.Uri), ct),
                    Source = s
                })
                .ToImmutableArray();

            await Task.WhenAll(queryCapacityTasks.Select(o => o.Task));

            var sourceClusterConfig = queryCapacityTasks
                .Select(o => new
                {
                    o.Source.Uri,
                    ConcurrentQueryCount = o.Source.ConcurrentQueryCount == 0
                    ? (int)Math.Max(1, 0.1 * o.Task.Result)
                    : o.Source.ConcurrentQueryCount
                });
            var sourceClusterQueryQueueMap = sourceClusterConfig
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoDbPriority>(o.ConcurrentQueryCount));
            var sourceClusterCommandQueueMap = sourceClusterConfig
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoDbPriority>(MAX_CONCURRENT_DB_COMMAND));
            var destinationClusterDmQueryQueueMap = parameterization.Activities
                .SelectMany(a => a.Destinations)
                .Select(d => NormalizedUri.NormalizeUri(d.ClusterUri))
                .Distinct()
                .ToImmutableDictionary(
                u => u,
                u => new PriorityExecutionQueue<KustoDbPriority>(MAX_CONCURRENT_DM_COMMAND));

            return new DbClientFactory(
                providerFactory,
                sourceClusterQueryQueueMap,
                sourceClusterCommandQueueMap,
                destinationClusterDmQueryQueueMap);
        }

        private DbClientFactory(
            ProviderFactory providerFactory,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterQueryQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> sourceClusterCommandQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> destinationClusterDmCommandQueueMap)
        {
            _providerFactory = providerFactory;
            _sourceClusterQueryQueueMap = sourceClusterQueryQueueMap;
            _sourceClusterCommandQueueMap = sourceClusterCommandQueueMap;
            _destinationClusterDmCommandQueueMap = destinationClusterDmCommandQueueMap;
        }

        private static async Task<int> GetQueryCapacityAsync(
            ICslAdminProvider provider,
            CancellationToken ct)
        {
            var commandText = @"
.show capacity
| where Resource == 'Queries'
| project Total";
            var reader = await provider.ExecuteControlCommandAsync(string.Empty, commandText);
            var queryCount = (long)reader.ToDataSet().Tables[0].Rows[0][0];

            return (int)queryCount;
        }
        #endregion

        void IDisposable.Dispose()
        {
            ((IDisposable)_providerFactory).Dispose();
        }

        public DbQueryClient GetDbQueryClient(Uri clusterUri, string database)
        {
            try
            {
                var queue = _sourceClusterQueryQueueMap[clusterUri];
                var provider = _providerFactory.GetQueryProvider(clusterUri);

                return new DbQueryClient(provider, queue, database);
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{clusterUri}'", false, ex);
            }
        }

        public DbCommandClient GetDbCommandClient(Uri clusterUri, string database)
        {
            try
            {
                var commandQueue = _sourceClusterCommandQueueMap[clusterUri];
                var provider = _providerFactory.GetDbCommandProvider(clusterUri);

                return new DbCommandClient(provider, commandQueue, database);
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{clusterUri}'", false, ex);
            }
        }

        public DmCommandClient GetDmCommandClient(Uri clusterUri, string database)
        {
            try
            {
                var queue = _destinationClusterDmCommandQueueMap[clusterUri];
                var provider = _providerFactory.GetDmCommandProvider(clusterUri);

                return new DmCommandClient(provider, queue, database);
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{clusterUri}'", false, ex);
            }
        }
    }
}