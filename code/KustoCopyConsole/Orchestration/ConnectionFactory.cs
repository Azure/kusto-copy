using Azure.Core;
using Azure.Identity;
using CsvHelper;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using KustoCopyConsole.JobParameter;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestration
{
    internal class ConnectionFactory
    {
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _commandProviderMap;
        private readonly ImmutableDictionary<Uri, ICslQueryProvider> _queryProviderMap;
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _dmCommandProviderMap;
        private readonly ImmutableDictionary<Uri, IKustoQueuedIngestClient> _dmIngestProviderMap;

        #region Constructor
        public ConnectionFactory(MainJobParameterization parameterization, TokenCredential credentials)
        {
            var sourceClusterUris = parameterization.SourceClusters
                .Select(s => NormalizedUri.NormalizeUri(s.SourceClusterUri))
                .Distinct();
            var destinationClusterUris = parameterization.SourceClusters
                .Select(s => s.Databases.Select(db => db.Destinations.Select(d => d.DestinationClusterUri)))
                .SelectMany(e => e)
                .SelectMany(e => e)
                .Select(s => NormalizedUri.NormalizeUri(s))
                .Distinct();
            var allClusterUris = sourceClusterUris
                .Concat(destinationClusterUris)
                .Distinct();
            var allBuilders = allClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = new KustoConnectionStringBuilder(uri.ToString())
                    .WithAadAzureTokenCredentialsAuthentication(credentials)
                });
            var destinationIngestionBuilders = allClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = new KustoConnectionStringBuilder(GetIngestUri(uri).ToString())
                    .WithAadAzureTokenCredentialsAuthentication(credentials)
                });

            _commandProviderMap = allBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslAdminProvider(e.Builder));
            _queryProviderMap = allBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslQueryProvider(e.Builder));
            _dmCommandProviderMap = destinationIngestionBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslAdminProvider(e.Builder));
            _dmIngestProviderMap = destinationIngestionBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoIngestFactory.CreateQueuedIngestClient(e.Builder));
        }
        #endregion

        public ICslAdminProvider GetCommandProvider(Uri clusterUri)
        {
            return _commandProviderMap[clusterUri];
        }

        public ICslQueryProvider GetQueryProvider(Uri clusterUri)
        {
            return _queryProviderMap[clusterUri];
        }

        public ICslAdminProvider GetDmCommandProvider(Uri clusterUri)
        {
            return _dmCommandProviderMap[clusterUri];
        }

        private static Uri GetIngestUri(Uri uri)
        {
            var builder = new UriBuilder(uri);

            builder.Host = $"ingest-{uri.Host}";

            return builder.Uri;
        }
    }
}