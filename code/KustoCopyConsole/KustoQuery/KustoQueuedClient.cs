using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Exceptions;
using KustoCopyConsole.Concurrency;
using Polly.Retry;
using Polly;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace KustoCopyConsole.KustoQuery
{
    public class KustoQueuedClient
    {
        #region Inner types
        private class InnerConfiguration
        {
            public InnerConfiguration(KustoClient kustoClient, int concurrentQueryCount)
            {
                Client = kustoClient;
                QueryExecutionQueue =
                    new PriorityExecutionQueue<KustoPriority>(concurrentQueryCount);
            }

            public KustoClient Client { get; }

            public PriorityExecutionQueue<KustoPriority> QueryExecutionQueue { get; }
        }
        #endregion

        private static readonly AsyncRetryPolicy _nonPermanentRetryPolicy = Policy
            .Handle<KustoException>(ex => !ex.IsPermanent)
            .WaitAndRetryForeverAsync(attempt =>
            TimeSpan.FromSeconds(Math.Max(10, attempt)),
            TraceNonPermanentException);

        private readonly InnerConfiguration _config;
        private readonly ClientRequestProperties _properties;
        private readonly AsyncPolicy? _policy;

        public KustoQueuedClient(KustoClient kustoClient, int concurrentQueryCount)
            : this(
                  new InnerConfiguration(kustoClient, concurrentQueryCount),
                  new ClientRequestProperties(),
                  _nonPermanentRetryPolicy)
        {
        }

        private KustoQueuedClient(
            InnerConfiguration config,
            ClientRequestProperties properties,
            AsyncPolicy? policy)
        {
            _config = config;
            _properties = properties;
            _policy = policy;
        }

        public KustoQueuedClient SetRetryPolicy(bool doRetry)
        {
            return new KustoQueuedClient(
                _config,
                _properties,
                doRetry ? _nonPermanentRetryPolicy : null);
        }

        public KustoQueuedClient SetParameter(string name, string value)
        {
            var newProperties = _properties.Clone();

            newProperties.SetParameter(name, value);

            return WithNewProperties(newProperties);
        }

        public KustoQueuedClient SetParameter(string name, DateTime value)
        {
            var newProperties = _properties.Clone();

            newProperties.SetParameter(name, value);

            return WithNewProperties(newProperties);
        }

        public KustoQueuedClient SetOption(string name, object value)
        {
            var newProperties = _properties.Clone();

            newProperties.SetOption(name, value);

            return WithNewProperties(newProperties);
        }

        private KustoQueuedClient WithNewProperties(ClientRequestProperties newProperties)
        {
            return new KustoQueuedClient(_config, newProperties, _policy);
        }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            KustoPriority priority,
            string database,
            string command,
            Func<IDataRecord, T> projection)
        {
            return await _config.QueryExecutionQueue.RequestRunAsync(
                priority,
                async () =>
                {
                    return await _config
                    .Client
                    .ExecuteCommandAsync(database, command, projection, _properties);
                });
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            KustoPriority priority,
            string database,
            string query,
            Func<IDataRecord, T> projection)
        {
            return await _config.QueryExecutionQueue.RequestRunAsync(priority, async () =>
            {
                return await _config
                .Client
                .ExecuteQueryAsync(database, query, projection, _properties);
            });
        }

        public async Task<(ImmutableArray<T>, ImmutableArray<U>)> ExecuteQueryAsync<T, U>(
            KustoPriority priority,
            string database,
            string query,
            Func<IDataRecord, T> projection1,
            Func<IDataRecord, U> projection2)
        {
            return await _config.QueryExecutionQueue.RequestRunAsync(priority, async () =>
            {
                return await _config
                .Client
                .ExecuteQueryAsync(database, query, projection1, projection2, _properties);
            });
        }

        private static void TraceNonPermanentException(Exception ex, TimeSpan ts)
        {
            Trace.TraceWarning(@$"Non permanent Kusto exception thrown.  It will be retried.
Exception:  {ex.GetType().Name}
Message:  {ex.Message}");
        }
    }
}