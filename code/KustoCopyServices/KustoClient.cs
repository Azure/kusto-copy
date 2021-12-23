using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using Kusto.Data.Net.Client;
using KustoCopyBookmarks;
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

namespace KustoCopyServices
{
    public class KustoClient
    {
        #region Inner Types
        private class ClientConfig
        {
            public ClientConfig(string clusterQueryUrl)
            {
                var clusterQueryUri = ValidateClusterQueryUri(clusterQueryUrl);
                var builder = new KustoConnectionStringBuilder(clusterQueryUri.ToString())
                    .WithAadUserPromptAuthentication();
                var commandProvider = KustoClientFactory.CreateCslCmAdminProvider(builder);
                var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);

                ClusterQueryUri = clusterQueryUri;
                QueryQueue = new ExecutionQueue(10);
                CommandQueue = new ExecutionQueue(2);
                CommandProvider = commandProvider;
                QueryProvider = queryProvider;
            }

            public Uri ClusterQueryUri { get; }

            public ExecutionQueue QueryQueue { get; }

            public ExecutionQueue CommandQueue { get; }

            public ICslAdminProvider CommandProvider { get; }

            public ICslQueryProvider QueryProvider { get; }
        }
        #endregion

        private static readonly AsyncRetryPolicy _retryPolicyThrottled =
            Policy.Handle<KustoRequestThrottledException>().WaitAndRetryAsync(
                5,
                attempt => TimeSpan.FromSeconds(attempt));
        private static readonly ClientRequestProperties EMPTY_REQUEST_PROPERTIES = new ClientRequestProperties();

        private readonly ClientConfig _config;
        private readonly ClientRequestProperties _properties;

        public KustoClient(string clusterQueryUrl)
        {
            _config = new ClientConfig(clusterQueryUrl);
            _properties = EMPTY_REQUEST_PROPERTIES;
        }

        private KustoClient(
            ClientConfig config,
            ClientRequestProperties properties)
        {
            _config = config;
            _properties = properties;
        }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            string database,
            string command,
            Func<IDataRecord, T> projection)
        {
            using (await _config.QueryQueue.RequestRunAsync())
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
                        + $"'{_config.ClusterQueryUri}', database '{database}' "
                        + $"for command '{command}'",
                        ex);
                }
            }
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            string database,
            string query,
            Func<IDataRecord, T> projection)
        {
            using (await _config.QueryQueue.RequestRunAsync())
            {
                try
                {
                    using (var reader = await ExecuteQueryWithPoliciesAsync(database, query))
                    {
                        var enumerableProjection = Project(reader, projection);

                        return enumerableProjection.ToImmutableArray();
                    }
                }
                catch (Exception ex)
                {
                    throw new CopyException(
                        $"Issue while executing a query in cluster '{_config.ClusterQueryUri}', "
                        + $"database '{database}':  '{query}'",
                        ex);
                }
            }
        }

        public async Task<(ImmutableArray<T>, ImmutableArray<U>)> ExecuteQueryAsync<T, U>(
            string database,
            string query,
            Func<IDataRecord, T> projection1,
            Func<IDataRecord, U> projection2)
        {
            using (await _config.QueryQueue.RequestRunAsync())
            {
                try
                {
                    using (var reader = await ExecuteQueryWithPoliciesAsync(database, query))
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
                catch (Exception ex)
                {
                    throw new CopyException(
                        $"Issue while executing a query in cluster '{_config.ClusterQueryUri}', "
                        + $"database '{database}':  '{query}'",
                        ex);
                }
            }
        }

        public KustoClient SetParameter(string name, string value)
        {
            var newProperties = _properties.Clone();

            newProperties.SetParameter(name, value);

            return WithNewProperties(newProperties);
        }

        public KustoClient SetParameter(string name, DateTime value)
        {
            var newProperties = _properties.Clone();

            newProperties.SetParameter(name, value);

            return WithNewProperties(newProperties);
        }

        private KustoClient WithNewProperties(ClientRequestProperties newProperties)
        {
            return new KustoClient(_config, newProperties);
        }

        private async Task<IDataReader> ExecuteCommandWithPoliciesAsync(
            string database,
            string command)
        {
            return await _retryPolicyThrottled.ExecuteAsync(async () =>
            {
                try
                {
                    return await _config.CommandProvider.ExecuteControlCommandAsync(
                        database,
                        command,
                        _properties);
                }
                catch (KustoRequestThrottledException)
                {
                    var parameters = _properties
                    .Parameters
                    .Select(p => $"'{p.Key}' : '{p.Value}'");
                    var paramList = string.Join(", ", parameters);

                    Trace.TraceWarning($"Kusto command throttled on db '{database}' "
                        + $"{{{paramList}}}:  '{command}'");

                    throw;
                }
            });
        }

        private async Task<IDataReader> ExecuteQueryWithPoliciesAsync(string database, string query)
        {
            return await _retryPolicyThrottled.ExecuteAsync(async () =>
            {
                try
                {
                    return await _config.QueryProvider.ExecuteQueryAsync(
                        database,
                        query,
                        _properties);
                }
                catch (KustoRequestThrottledException)
                {
                    var parameters = _properties
                    .Parameters
                    .Select(p => $"'{p.Key}' : '{p.Value}'");
                    var paramList = string.Join(", ", parameters);

                    Trace.TraceWarning($"Kusto query throttled on db '{database}' "
                        + $"{{{paramList}}}:  '{query}'");

                    throw;
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