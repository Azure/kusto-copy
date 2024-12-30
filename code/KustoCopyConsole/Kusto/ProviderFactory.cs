using Azure.Core;
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
        private const string APPLICATION_NAME = "KustoCopy";

        private readonly ImmutableDictionary<Uri, ICslQueryProvider> _queryProviderMap;
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _commandProviderMap;
        private readonly ImmutableDictionary<Uri, ICslAdminProvider> _dmCommandProviderMap;

        #region Constructor
        public ProviderFactory(MainJobParameterization parameterization, TokenCredential credentials)
        {
            var sourceClusterUris = parameterization.Activities
                .Select(a => NormalizedUri.NormalizeUri(a.Source.ClusterUri))
                .Distinct();
            var destinationClusterUris = parameterization.Activities
                .SelectMany(a => a.Destinations)
                .Select(d => NormalizedUri.NormalizeUri(d.ClusterUri))
                .Distinct();
            var allClusterUris = sourceClusterUris
                .Concat(destinationClusterUris)
                .Distinct();
            var sourceBuilders = sourceClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, uri)
                });
            var destinationIngestionBuilders = destinationClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, GetIngestUri(uri))
                });
            var allBuilders = allClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, uri)
                });

            _queryProviderMap = allBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslQueryProvider(e.Builder));
            _commandProviderMap = allBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslAdminProvider(e.Builder));
            _dmCommandProviderMap = destinationIngestionBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoClientFactory.CreateCslAdminProvider(e.Builder));
        }

        private static KustoConnectionStringBuilder CreateBuilder(
            TokenCredential credentials,
            Uri uri)
        {
            var builder = new KustoConnectionStringBuilder(uri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            builder.ApplicationNameForTracing = "KUSTO-COPY";

            return builder;
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

        public ICslQueryProvider GetQueryProvider(Uri clusterUri)
        {
            return _queryProviderMap[clusterUri];
        }

        public ICslAdminProvider GetCommandProvider(Uri clusterUri)
        {
            return _commandProviderMap[clusterUri];
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