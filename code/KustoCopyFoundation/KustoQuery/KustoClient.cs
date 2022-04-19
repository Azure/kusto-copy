using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Data.Net.Client;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation.KustoQuery
{
    public class KustoClient
    {
        private static readonly AsyncRetryPolicy _retryPolicyThrottled =
            Policy.Handle<KustoRequestThrottledException>().WaitAndRetryAsync(
                5,
                attempt => TimeSpan.FromSeconds(attempt));

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
                using (var reader = await ExecuteCommandWithPoliciesAsync(database, command))
                {
                    var enumerableProjection = Project(reader, projection);

                    return enumerableProjection.ToImmutableArray();
                }
            }
            catch (Exception ex)
            {
                throw new CopyException(
                    "Issue while executing a command in cluster "
                    + $"'{_clusterQueryUri}', database '{database}' "
                    + $"for command '{command}'",
                    ex);
            }
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            string database,
            string query,
            Func<IDataRecord, T> projection,
            ClientRequestProperties properties)
        {
            using (var reader = await ExecuteQueryWithPoliciesAsync(database, query, properties))
            {
                var enumerableProjection = Project(reader, projection);

                return enumerableProjection.ToImmutableArray();
            }
        }

        public async Task<(ImmutableArray<T>, ImmutableArray<U>)> ExecuteQueryAsync<T, U>(
            string database,
            string query,
            Func<IDataRecord, T> projection1,
            Func<IDataRecord, U> projection2,
            ClientRequestProperties properties)
        {
            using (var reader = await ExecuteQueryWithPoliciesAsync(database, query, properties))
            {
                var enumerableProjection1 = Project(reader, projection1).ToImmutableArray();

                if (!reader.NextResult())
                {
                    throw new CopyException("Query result doesn't contain a second result");
                }

                var enumerableProjection2 = Project(reader, projection2).ToImmutableArray();

                return (
                    enumerableProjection1.ToImmutableArray(),
                    enumerableProjection2.ToImmutableArray());
            }
        }

        private async Task<IDataReader> ExecuteCommandWithPoliciesAsync(
            string database,
            string command)
        {
            return await _retryPolicyThrottled.ExecuteAsync(async () =>
            {
                try
                {
                    return await _commandProvider.ExecuteControlCommandAsync(
                        database,
                        command);
                }
                catch (KustoRequestThrottledException)
                {
                    Trace.TraceWarning(
                        $"Kusto command throttled on db '{database}':  '{command}'");

                    throw;
                }
            });
        }

        private async Task<IDataReader> ExecuteQueryWithPoliciesAsync(
            string database,
            string query,
            ClientRequestProperties properties)
        {
            return await _retryPolicyThrottled.ExecuteAsync(async () =>
            {
                try
                {
                    return await _queryProvider.ExecuteQueryAsync(
                        database,
                        query,
                        properties);
                }
                catch (KustoRequestThrottledException)
                {
                    var parameters = properties
                    .Parameters
                    .Select(p => $"'{p.Key}' : '{p.Value}'");
                    var paramList = string.Join(", ", parameters);

                    Trace.TraceWarning($"Kusto query throttled on db '{database}' "
                        + $"{{{paramList}}}:  '{query}'");

                    throw;
                }
                catch (Exception ex)
                {
                    throw new CopyException(
                        $"Issue while executing a query in cluster '{_clusterQueryUri}', "
                        + $"database '{database}':  '{query}'",
                        ex);
                }
            });
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