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
using KustoCopyConsole.Orchestrations;

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

        private readonly InnerConfiguration _config;
        private readonly ClientRequestProperties _properties;
        private readonly bool _doRetry;

        public KustoQueuedClient(KustoClient kustoClient, int concurrentQueryCount)
            : this(
                  new InnerConfiguration(kustoClient, concurrentQueryCount),
                  new ClientRequestProperties(),
                  true)
        {
        }

        private KustoQueuedClient(
            InnerConfiguration config,
            ClientRequestProperties properties,
            bool doRetry)
        {
            _config = config;
            _properties = properties;
            _doRetry = doRetry;
        }

        public string HostName => _config.Client.HostName;

        public KustoQueuedClient SetRetryPolicy(bool doRetry)
        {
            return new KustoQueuedClient(
                _config,
                _properties,
                doRetry);
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
            return new KustoQueuedClient(_config, newProperties, _doRetry);
        }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            KustoPriority priority,
            string database,
            string command,
            Func<IDataRecord, T> projection)
        {
            return await ExecuteAsync(async (ct) =>
            {
                return await _config.QueryExecutionQueue.RequestRunAsync(
                    priority,
                    async () =>
                    {
                        return await _config
                        .Client
                        .ExecuteCommandAsync(database, command, projection, _properties);
                    });
            });
        }

        public async Task<ImmutableArray<T>> ExecuteQueryAsync<T>(
            KustoPriority priority,
            string database,
            string query,
            Func<IDataRecord, T> projection)
        {
            return await ExecuteAsync(async (ct) =>
            {
                return await _config.QueryExecutionQueue.RequestRunAsync(priority, async () =>
                {
                    return await _config
                    .Client
                    .ExecuteQueryAsync(database, query, projection, _properties);
                });
            });
        }

        public async Task<(ImmutableArray<T>, ImmutableArray<U>)> ExecuteQueryAsync<T, U>(
            KustoPriority priority,
            string database,
            string query,
            Func<IDataRecord, T> projection1,
            Func<IDataRecord, U> projection2)
        {
            return await ExecuteAsync(async (ct) =>
            {
                return await _config.QueryExecutionQueue.RequestRunAsync(priority, async () =>
                {
                    return await _config
                    .Client
                    .ExecuteQueryAsync(database, query, projection1, projection2, _properties);
                });
            });
        }

        private static void TraceNonPermanentException(Exception ex, TimeSpan ts)
        {
            Trace.TraceWarning(@$"Non permanent Kusto exception thrown.  It will be retried.
Exception:  {ex.GetType().Name}
Message:  {ex.Message}");
        }

        private async Task<T> ExecuteAsync<T>(
            Func<CancellationToken, Task<T>> action,
            CancellationToken ct = default(CancellationToken))
        {
            if (_doRetry)
            {
                return await RetryHelper.RetryNonPermanentKustoErrorPolicy.ExecuteAsync(
                    action,
                    ct);
            }
            else
            {
                return await action(ct);
            }
        }
    }
}