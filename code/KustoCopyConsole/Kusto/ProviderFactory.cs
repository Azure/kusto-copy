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
        private readonly ImmutableDictionary<Uri, IKustoQueuedIngestClient> _ingestProviderMap;

        #region Constructor
        public ProviderFactory(
            MainJobParameterization parameterization,
            TokenCredential credentials,
            string traceApplicationName)
        {
            var sourceClusterUris = parameterization.Activities
                .Values
                .Select(a => a.GetSourceTableIdentity().ClusterUri)
                .Distinct();
            var destinationClusterUris = parameterization.Activities
                .Values
                .Select(a => a.GetDestinationTableIdentity().ClusterUri)
                .Distinct();
            var allClusterUris = sourceClusterUris
                .Concat(destinationClusterUris)
                .Distinct();
            var sourceBuilders = sourceClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, uri, traceApplicationName)
                });
            var destinationIngestionBuilders = destinationClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, GetIngestUri(uri), traceApplicationName)
                });
            var allBuilders = allClusterUris
                .Select(uri => new
                {
                    Uri = uri,
                    Builder = CreateBuilder(credentials, uri, traceApplicationName)
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
            _ingestProviderMap = destinationIngestionBuilders
                .ToImmutableDictionary(
                e => e.Uri,
                e => KustoIngestFactory.CreateQueuedIngestClient(e.Builder));
        }

        private static KustoConnectionStringBuilder CreateBuilder(
            TokenCredential credentials,
            Uri uri,
            string traceApplicationName)
        {
            var builder = new KustoConnectionStringBuilder(uri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credentials);

            builder.ApplicationNameForTracing = traceApplicationName;

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

        public IKustoQueuedIngestClient GetIngestProvider(Uri clusterUri)
        {
            return _ingestProviderMap[clusterUri];
        }

        private static Uri GetIngestUri(Uri uri)
        {
            var builder = new UriBuilder(uri);

            builder.Host = $"ingest-{uri.Host}";

            return builder.Uri;
        }
    }
}