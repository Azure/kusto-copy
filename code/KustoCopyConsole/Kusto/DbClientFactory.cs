using Azure.Core;
using Azure.Identity;
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
        private const int MAX_CONCURRENT_DB_COMMAND = 10;

        private readonly ProviderFactory _providerFactory;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _allClusterQueryQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _allClusterCommandQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> _destinationClusterDmCommandQueueMap;
        private readonly IImmutableDictionary<Uri, ExportCoreClient> _sourceClusterExportCoreMap;

        #region Constructor
        public static async Task<DbClientFactory> CreateAsync(
            MainJobParameterization parameterization,
            TokenCredential credentials,
            CancellationToken ct)
        {
            var providerFactory = new ProviderFactory(parameterization, credentials);
            var sourceClusterUris = parameterization.Activities
                .Select(a => NormalizedUri.NormalizeUri(a.Source.ClusterUri))
                .Distinct();
            var destinationClusterUris = parameterization.Activities
                .Select(a => NormalizedUri.NormalizeUri(a.Destination.ClusterUri))
                .Distinct();
            var allClusterUris = sourceClusterUris
                .Concat(destinationClusterUris)
                .Distinct();
            var capacityTasks = allClusterUris
                .Select(uri => new
                {
                    Task = GetCapacitiesAsync(providerFactory.GetCommandProvider(uri), ct),
                    Uri = uri
                })
                .ToImmutableArray();

            await Task.WhenAll(capacityTasks.Select(o => o.Task));

            var allClusterQueryCount = capacityTasks
                .Select(o => new
                {
                    o.Uri,
                    ConcurrentQueryCount = (int)Math.Max(1, 0.1 * o.Task.Result.query)
                });
            var allClusterQueryQueueMap = allClusterQueryCount
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoDbPriority>(o.ConcurrentQueryCount));
            var allClusterCommandQueueMap = allClusterQueryCount
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoDbPriority>(MAX_CONCURRENT_DB_COMMAND));
            var destinationClusterDmQueryQueueMap = destinationClusterUris
                .ToImmutableDictionary(
                u => u,
                u => new PriorityExecutionQueue<KustoDbPriority>(MAX_CONCURRENT_DM_COMMAND));
            var sourceClusterExportCoreMap = capacityTasks
                .Where(o => sourceClusterUris.Contains(o.Uri))
                .Select(o => new
                {
                    o.Uri,
                    Client = new ExportCoreClient(
                        new DbCommandClient(
                            providerFactory.GetCommandProvider(o.Uri),
                            allClusterCommandQueueMap[o.Uri],
                            string.Empty),
                        o.Task.Result.export)
                })
                .ToImmutableDictionary(o => o.Uri, o => o.Client);

            return new DbClientFactory(
                providerFactory,
                allClusterQueryQueueMap,
                allClusterCommandQueueMap,
                destinationClusterDmQueryQueueMap,
                sourceClusterExportCoreMap);
        }

        private DbClientFactory(
            ProviderFactory providerFactory,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> allClusterQueryQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> allClusterCommandQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoDbPriority>> destinationClusterDmCommandQueueMap,
            IImmutableDictionary<Uri, ExportCoreClient> sourceClusterExportCoreMap)
        {
            _providerFactory = providerFactory;
            _allClusterQueryQueueMap = allClusterQueryQueueMap;
            _allClusterCommandQueueMap = allClusterCommandQueueMap;
            _destinationClusterDmCommandQueueMap = destinationClusterDmCommandQueueMap;
            _sourceClusterExportCoreMap = sourceClusterExportCoreMap;
        }

        private static async Task<(int query, int export)> GetCapacitiesAsync(
            ICslAdminProvider provider,
            CancellationToken ct)
        {
            var commandText = @"
.show capacity
| where Resource in ('Queries', 'DataExport')
| project Resource, Total";
            var reader = await provider.ExecuteControlCommandAsync(string.Empty, commandText);
            var capacityMap = reader.ToDataSet().Tables[0].Rows
                .Cast<DataRow>()
                .Select(r => new
                {
                    Resource = (string)r[0],
                    Total = (long)r[1]
                })
                .ToImmutableDictionary(r => r.Resource, r => r.Total);

            return ((int)capacityMap["Queries"], (int)capacityMap["DataExport"]);
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
                var queue = _allClusterQueryQueueMap[clusterUri];
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
                var commandQueue = _allClusterCommandQueueMap[clusterUri];
                var provider = _providerFactory.GetCommandProvider(clusterUri);

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

        public ExportClient GetExportClient(Uri clusterUri, string database, string table)
        {
            try
            {
                var exportCoreClient = _sourceClusterExportCoreMap[clusterUri];
                var dbClient = GetDbCommandClient(clusterUri, database);

                return new ExportClient(exportCoreClient, dbClient, table);
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{clusterUri}'", false, ex);
            }
        }

        public IngestClient GetIngestClient(Uri clusterUri, string database, string table)
        {
            try
            {
                var ingestProvider = _providerFactory.GetIngestProvider(clusterUri);
                var ingestClient = new IngestClient(ingestProvider, database, table);

                return ingestClient;
            }
            catch (KeyNotFoundException ex)
            {
                throw new CopyException($"Can't find cluster '{clusterUri}'", false, ex);
            }
        }
    }
}