using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;
using System.Net.NetworkInformation;

namespace KustoCopyConsole.Orchestrations
{
    public class ConnectionsFactory
    {
        #region Constructors
        public static async Task<ConnectionsFactory> CreateAsync(
            MainParameterization parameterization)
        {
            var lakeFolderBuilder =
                new KustoConnectionStringBuilder(parameterization.LakeFolderConnectionString);
            var credentials = CreateCredentials(lakeFolderBuilder);
            var lakeFolderUri = new Uri(lakeFolderBuilder.DataSource);
            var lakeFolderClient = new DataLakeDirectoryClient(lakeFolderUri, credentials);
            var lakeContainerClient = GetLakeContainerClient(lakeFolderUri, credentials);
            var sourceExportQueueTask = parameterization.Source != null
                ? CreateKustoExportQueueAsync(
                    credentials,
                    parameterization.Source!.ClusterQueryConnectionString!,
                    parameterization.Source!.ConcurrentQueryCount,
                    parameterization.Source!.ConcurrentExportCommandCount)
                : null;
            var destinationIngestQueueTask = parameterization.Destination != null
                ? CreateKustoIngestQueueAsync(
                    credentials,
                    parameterization.Destination!.ClusterQueryConnectionString!,
                    parameterization.Destination!.ConcurrentQueryCount,
                    parameterization.Destination!.ConcurrentIngestionCount)
                : null;

            //  Forces authentication to data lake
            await lakeContainerClient.GetPropertiesAsync();

            var sourceExportQueue = sourceExportQueueTask == null
                ? null
                : await sourceExportQueueTask;
            var destinationIngestQueue = destinationIngestQueueTask == null
                ? null
                : await destinationIngestQueueTask;

            return new ConnectionsFactory(
                lakeFolderClient,
                lakeContainerClient,
                sourceExportQueue,
                destinationIngestQueue);
        }

        private ConnectionsFactory(
            DataLakeDirectoryClient lakeFolderClient,
            BlobContainerClient lakeContainerClient,
            KustoExportQueue? sourceExportQueue,
            KustoIngestQueue? destinationIngestQueue)
        {
            LakeFolderClient = lakeFolderClient;
            LakeContainerClient = lakeContainerClient;
            SourceExportQueue = sourceExportQueue;
            DestinationIngestQueue = destinationIngestQueue;
        }

        #region Helpers
        private static BlobContainerClient GetLakeContainerClient(
            Uri lakeFolderUri,
            TokenCredential credentials)
        {
            var builder = new BlobUriBuilder(lakeFolderUri);

            //  Enforce blob storage API
            builder.Host =
                builder.Host.Replace(".dfs.core.windows.net", ".blob.core.windows.net");
            lakeFolderUri = builder.ToUri();

            var blobClient = new BlobClient(lakeFolderUri, credentials);
            var containerClient = blobClient.GetParentBlobContainerClient();

            return containerClient;
        }

        private static async Task<KustoQueuedClient> CreateKustoQueuedClientAsync(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount)
        {
            var sourceBuilder = new KustoConnectionStringBuilder(clusterQueryConnectionString);
            var sourceKustoClient = new KustoClient(
                sourceBuilder.WithAadAzureTokenCredentialsAuthentication(credentials));
            var actualConcurrentQueryCount = concurrentQueryCount == 0
                ? await FetchQueryCountAsync(sourceKustoClient)
                : concurrentQueryCount;
            var sourceQueuedClient = new KustoQueuedClient(
                sourceKustoClient,
                actualConcurrentQueryCount);

            return sourceQueuedClient;
        }

        private static async Task<int> FetchQueryCountAsync(KustoClient client)
        {
            //  We create a queued client to beneficiate from retries
            var queuedClient = new KustoQueuedClient(client, 1);
            var commandText = ".show capacity queries | project Total";
            var totals = await queuedClient.ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                string.Empty,
                commandText,
                r => (long)r["Total"]);
            var total = (int)totals.First();
            var actual = Math.Min(1, total / 10);

            return actual;
        }

        private static async Task<KustoExportQueue> CreateKustoExportQueueAsync(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount,
            int concurrentExportCommandCount)
        {
            var queuedClient = await CreateKustoQueuedClientAsync(
                credentials,
                clusterQueryConnectionString,
                concurrentQueryCount);
            var awaiter = new KustoOperationAwaiter(queuedClient);
            var effectiveExportCommandCount = concurrentExportCommandCount <= 0
                ? await FetchMaxExportCommandCountAsync(queuedClient)
                : concurrentExportCommandCount;
            var exportQueue = new KustoExportQueue(
                queuedClient,
                awaiter,
                effectiveExportCommandCount);

            return exportQueue;
        }

        private static async Task<int> FetchMaxExportCommandCountAsync(
            KustoQueuedClient queuedClient)
        {
            var commandText = ".show capacity data-export | project Total";
            var totals = await queuedClient.ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                string.Empty,
                commandText,
                r => (long)r["Total"]);
            var total = (int)totals.First();

            return total;
        }

        private static async Task<KustoIngestQueue> CreateKustoIngestQueueAsync(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount,
            int concurrentIngestionCount)
        {
            var queuedClient = await CreateKustoQueuedClientAsync(
                credentials,
                clusterQueryConnectionString,
                concurrentQueryCount);
            var awaiter = new KustoOperationAwaiter(queuedClient);
            var effectiveIngestionCount = concurrentIngestionCount <= 0
                ? await FetchMaxIngestionCountAsync(queuedClient)
                : concurrentIngestionCount;
            var ingestQueue = new KustoIngestQueue(
                queuedClient,
                awaiter,
                effectiveIngestionCount);

            return ingestQueue;
        }

        private static async Task<int> FetchMaxIngestionCountAsync(KustoQueuedClient queuedClient)
        {
            var commandText = ".show capacity ingestions | project Total";
            var totals = await queuedClient.ExecuteCommandAsync(
                KustoPriority.HighestPriority,
                string.Empty,
                commandText,
                r => (long)r["Total"]);
            var total = (int)totals.First();

            return total;
        }

        private static TokenCredential CreateCredentials(KustoConnectionStringBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(builder.ApplicationClientId))
            {
                return new DefaultAzureCredential();
            }
            else
            {
                if (string.IsNullOrEmpty(builder.Authority))
                {
                    throw new CopyException(
                        $"{nameof(builder.ApplicationClientId)} is specified"
                        + $" but {builder.Authority} isn't");
                }
                if (string.IsNullOrEmpty(builder.ApplicationKey))
                {
                    throw new CopyException(
                        $"{nameof(builder.ApplicationClientId)} is specified"
                        + $" but {builder.ApplicationKey} isn't");
                }

                return new ClientSecretCredential(
                    builder.Authority,
                    builder.ApplicationClientId,
                    builder.ApplicationKey);
            }
        }
        #endregion
        #endregion

        public DataLakeDirectoryClient LakeFolderClient { get; }

        public BlobContainerClient LakeContainerClient { get; }

        public KustoExportQueue? SourceExportQueue { get; }

        public KustoIngestQueue? DestinationIngestQueue { get; }
    }
}