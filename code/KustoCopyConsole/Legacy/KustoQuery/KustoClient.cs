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

namespace KustoCopyConsole.KustoQuery
{
    public class KustoClient
    {
        private static readonly ClientRequestProperties EMPTY_REQUEST_PROPERTIES =
            new ClientRequestProperties();

        private readonly ICslAdminProvider _commandProvider;
        private readonly ICslQueryProvider _queryProvider;

        public KustoClient(KustoConnectionStringBuilder builder)
        {
            var commandProvider = KustoClientFactory.CreateCslAdminProvider(builder);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);

            HostName = builder.Hostname;
            _commandProvider = commandProvider;
            _queryProvider = queryProvider;
        }

        public string HostName { get; }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            string database,
            string command,
            Func<IDataRecord, T> projection,
            ClientRequestProperties? properties = null)
        {
            properties = properties ?? EMPTY_REQUEST_PROPERTIES;

            using (var reader = await _commandProvider.ExecuteControlCommandAsync(
                database,
                command,
                properties))
            {
                var enumerableProjection = Project(reader, projection);

                return enumerableProjection.ToImmutableArray();
            }
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            string database,
            string query,
            Func<IDataRecord, T> projection,
            ClientRequestProperties? properties = null)
        {
            properties = properties ?? EMPTY_REQUEST_PROPERTIES;

            using (var reader = await _queryProvider.ExecuteQueryAsync(
                database,
                query,
                properties))
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
            ClientRequestProperties? properties = null)
        {
            properties = properties ?? EMPTY_REQUEST_PROPERTIES;

            using (var reader = await _queryProvider.ExecuteQueryAsync(
                database,
                query,
                properties))
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

        private static IEnumerable<T> Project<T>(
            IDataReader reader,
            Func<IDataRecord, T> projection)
        {
            while (reader.Read())
            {
                yield return projection(reader);
            }
        }
    }
}