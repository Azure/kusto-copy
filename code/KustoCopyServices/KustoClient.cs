using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using KustoCopyBookmarks;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class KustoClient
    {
        private static readonly ClientRequestProperties EMPTY_REQUEST_PROPERTIES = new ClientRequestProperties();

        private readonly Uri _clusterQueryUri;
        private readonly ICslAdminProvider _commandProvider;
        private readonly ICslQueryProvider _queryProvider;

        public KustoClient(string clusterQueryUrl)
        {
            var clusterQueryUri = ValidateClusterQueryUri(clusterQueryUrl);
            var builder = new KustoConnectionStringBuilder(clusterQueryUri.ToString())
                .WithAadUserPromptAuthentication();
            var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);

            _clusterQueryUri = clusterQueryUri;
            _commandProvider = commandProvider;
            _queryProvider = queryProvider;
        }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            string database,
            string command,
            Func<IDataRecord, T> projection)
        {
            try
            {
                using (var reader =
                    await _commandProvider.ExecuteControlCommandAsync(database, command))
                {
                    var enumerableProjection = Project(reader, projection);

                    return enumerableProjection.ToImmutableArray();
                }
            }
            catch (Exception ex)
            {
                throw new CopyException(
                    $"Issue while executing a command in cluster '{_clusterQueryUri}', "
                    + $"database '{database}':  '{command}'",
                    ex);
            }
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            string database,
            string query,
            Func<IDataRecord, T> projection)
        {
            try
            {
                using (var reader = await _queryProvider.ExecuteQueryAsync(
                    database,
                    query,
                    EMPTY_REQUEST_PROPERTIES))
                {
                    var enumerableProjection = Project(reader, projection);

                    return enumerableProjection.ToImmutableArray();
                }
            }
            catch (Exception ex)
            {
                throw new CopyException(
                    $"Issue while executing a query in cluster '{_clusterQueryUri}', "
                    + $"database '{database}':  '{query}'",
                    ex);
            }
        }

        private static IEnumerable<T> Project<T>(
            IDataReader reader,
            Func<IDataRecord, T> projection)
        {
            while (reader.Read())
            {
                yield return projection(reader);
            }
        }

        private static Uri ValidateClusterQueryUri(string clusterQueryUrl)
        {
            Uri? clusterUri;

            if (Uri.TryCreate(clusterQueryUrl, UriKind.Absolute, out clusterUri))
            {
                return clusterUri;
            }
            else
            {
                throw new CopyException($"Invalid cluster query uri:  '{clusterQueryUrl}'");
            }
        }
    }
}