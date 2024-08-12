using Azure.Core;
using Azure.Identity;
using CsvHelper;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using KustoCopyConsole.JobParameter;
using System.Collections.Immutable;

namespace KustoCopyConsole.Kusto
{
    internal class ProviderFactory : IDisposable
    {
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _commandProviderMap;
        private readonly ImmutableDictionary<Uri, ICslQueryProvider> _queryProviderMap;
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _dmCommandProviderMap;

        #region Constructor
        public ProviderFactory(MainJobParameterization parameterization, TokenCredential credentials)
        {
            var sourceClusterUris = parameterization.Activities
                .Select(a => NormalizedUri.NormalizeUri(a.Source.ClusterUri))
                .Distinct();
            var sourceBuilders = sourceClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = new KustoConnectionStringBuilder(uri.ToString())
                    .WithAadAzureTokenCredentialsAuthentication(credentials)
                });
            var destinationClusterUris = parameterization.Activities
                .SelectMany(a => a.Destinations)
                .Select(d => NormalizedUri.NormalizeUri(d.ClusterUri))
                .Distinct();
            var destinationIngestionBuilders = destinationClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = new KustoConnectionStringBuilder(GetIngestUri(uri).ToString())
                    .WithAadAzureTokenCredentialsAuthentication(credentials)
                });
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

            _commandProviderMap = sourceBuilders
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
        }
        #endregion

        void IDisposable.Dispose()
        {
            var disposables = _commandProviderMap.Values.Cast<IDisposable>()
                .Concat(_queryProviderMap.Values)
                .Concat(_dmCommandProviderMap.Values);

            foreach (var disposable in disposables)
            {
                disposable.Dispose();
            }
        }

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