﻿using Azure.Core;
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

        private readonly ProviderFactory _providerFactory;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> _allClusterQueryQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> _allClusterCommandQueueMap;
        private readonly IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> _destinationClusterDmCommandQueueMap;

        #region Constructor
        public static async Task<DbClientFactory> CreateAsync(
            MainJobParameterization parameterization,
            TokenCredential credentials,
            CancellationToken ct)
        {
            var providerFactory = new ProviderFactory(parameterization, credentials);
            var sourceClusterUris = parameterization.Activities
                .Values
                .Select(a => NormalizedUri.NormalizeUri(a.Source.ClusterUri))
                .Distinct();
            var destinationClusterUris = parameterization.Activities
                .Values
                .Select(a => NormalizedUri.NormalizeUri(a.Destination.ClusterUri))
                .Distinct();
            var allClusterUris = sourceClusterUris
                .Concat(destinationClusterUris)
                .Distinct();
            var queryCapacityTasks = allClusterUris
                .Select(uri => new
                {
                    Task = GetQueryCapacityAsync(providerFactory.GetCommandProvider(uri), ct),
                    Uri = uri
                })
                .ToImmutableArray();

            await Task.WhenAll(queryCapacityTasks.Select(o => o.Task));

            var allClusterQueryCount = queryCapacityTasks
                .Select(o => new
                {
                    o.Uri,
                    ConcurrentQueryCount = (int)Math.Max(1, 0.1 * o.Task.Result)
                });
            var allClusterQueryQueueMap = allClusterQueryCount
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoPriority>(o.ConcurrentQueryCount));
            var allClusterCommandQueueMap = allClusterQueryCount
                .ToImmutableDictionary(
                o => o.Uri,
                o => new PriorityExecutionQueue<KustoPriority>(o.ConcurrentQueryCount));
            var destinationClusterDmQueryQueueMap = destinationClusterUris
                .ToImmutableDictionary(
                u => u,
                u => new PriorityExecutionQueue<KustoPriority>(MAX_CONCURRENT_DM_COMMAND));

            return new DbClientFactory(
                providerFactory,
                allClusterQueryQueueMap,
                allClusterCommandQueueMap,
                destinationClusterDmQueryQueueMap);
        }

        private DbClientFactory(
            ProviderFactory providerFactory,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> allClusterQueryQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> allClusterCommandQueueMap,
            IImmutableDictionary<Uri, PriorityExecutionQueue<KustoPriority>> destinationClusterDmCommandQueueMap)
        {
            _providerFactory = providerFactory;
            _allClusterQueryQueueMap = allClusterQueryQueueMap;
            _allClusterCommandQueueMap = allClusterCommandQueueMap;
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
            var capacity = reader.ToDataSet().Tables[0].Rows
                .Cast<DataRow>()
                .Select(r => (long)r[0])
                .First();

            return (int)capacity;
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