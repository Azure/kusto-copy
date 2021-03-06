using Kusto.Data.Common;
using KustoCopyBookmarks;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public class KustoQueuedClient
    {
        #region Inner types
        private class InnerConfiguration
        {
            public InnerConfiguration(
                string clusterQueryUrl,
                int concurrentQueryCount,
                int concurrentExportCommandCount)
            {
                Client = new KustoClient(clusterQueryUrl);
                QueryExecutionQueue =
                    new PriorityExecutionQueue<KustoPriority>(concurrentQueryCount);
                ExportCommandExecutionQueue = new ExecutionQueue(concurrentExportCommandCount);
            }

            public KustoClient Client { get; }

            public PriorityExecutionQueue<KustoPriority> QueryExecutionQueue { get; }

            public ExecutionQueue ExportCommandExecutionQueue { get; }
        }
        #endregion

        private static readonly ClientRequestProperties EMPTY_REQUEST_PROPERTIES = new ClientRequestProperties();

        private readonly InnerConfiguration _config;
        private readonly ClientRequestProperties _properties;

        public KustoQueuedClient(
            string clusterQueryUrl,
            int concurrentQueryCount,
            int concurrentExportCommandCount)
        {
            _config = new InnerConfiguration(
                clusterQueryUrl,
                concurrentQueryCount,
                concurrentExportCommandCount);
            _properties = new ClientRequestProperties();
        }

        private KustoQueuedClient(InnerConfiguration config, ClientRequestProperties properties)
        {
            _config = config;
            _properties = properties;
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

        private KustoQueuedClient WithNewProperties(ClientRequestProperties newProperties)
        {
            return new KustoQueuedClient(_config, newProperties);
        }

        public async Task<ImmutableArray<T>> ExecuteCommandAsync<T>(
            KustoPriority priority,
            string database,
            string command,
            Func<IDataRecord, T> projection)
        {
            if (!priority.IsExportRelated)
            {
                return await _config.QueryExecutionQueue.RequestRunAsync(priority, async () =>
                {
                    return await _config
                    .Client
                    .ExecuteCommandAsync(database, command, projection);
                });
            }
            else
            {
                return await _config.ExportCommandExecutionQueue.RequestRunAsync(async () =>
                {
                    return await _config
                    .Client
                    .ExecuteCommandAsync(database, command, projection);
                });
            }
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
    }
}