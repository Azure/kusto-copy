using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;

namespace KustoCopyConsole.Orchestrations
{
    public class ConnectionsFactory
    {
        #region Constructors
        public static ConnectionsFactory Create(MainParameterization parameterization)
        {
            var lakeFolderBuilder =
                new KustoConnectionStringBuilder(parameterization.LakeFolderConnectionString);
            var credentials = CreateCredentials(lakeFolderBuilder);
            var lakeFolderUri = new Uri(lakeFolderBuilder.DataSource);
            var lakeFolderClient = new DataLakeDirectoryClient(lakeFolderUri, credentials);
            var lakeContainerClient = GetLakeContainerClient(lakeFolderUri, credentials);
            var sourceExportQueue = parameterization.Source != null
                ? CreateKustoExportQueue(
                    credentials,
                    parameterization.Source!.ClusterQueryConnectionString!,
                    parameterization.Source!.ConcurrentQueryCount,
                    parameterization.Source!.ConcurrentExportCommandCount)
                : null;
            var destinationIngestQueue = parameterization.Destination != null
                ? CreateKustoIngestQueue(
                    credentials,
                    parameterization.Destination!.ClusterQueryConnectionString!,
                    parameterization.Destination!.ConcurrentQueryCount,
                    parameterization.Destination!.ConcurrentIngestionCount)
                : null;

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

        private static KustoQueuedClient CreateKustoQueuedClient(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount)
        {
            var sourceBuilder = new KustoConnectionStringBuilder(clusterQueryConnectionString);
            var sourceKustoClient = new KustoClient(
                sourceBuilder.WithAadAzureTokenCredentialsAuthentication(credentials));
            var sourceQueuedClient = new KustoQueuedClient(
                sourceKustoClient,
                concurrentQueryCount);

            return sourceQueuedClient;
        }

        private static KustoExportQueue CreateKustoExportQueue(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount,
            int concurrentExportCommandCount)
        {
            var queuedClient = CreateKustoQueuedClient(
                credentials,
                clusterQueryConnectionString,
                concurrentQueryCount);
            var awaiter = new KustoOperationAwaiter(queuedClient);
            var exportQueue = new KustoExportQueue(
                queuedClient,
                awaiter,
                concurrentExportCommandCount);

            return exportQueue;
        }

        private static KustoIngestQueue CreateKustoIngestQueue(
            TokenCredential credentials,
            string clusterQueryConnectionString,
            int concurrentQueryCount,
            int concurrentIngestionCount)
        {
            var queuedClient = CreateKustoQueuedClient(
                credentials,
                clusterQueryConnectionString,
                concurrentQueryCount);
            var awaiter = new KustoOperationAwaiter(queuedClient);
            var ingestQueue = new KustoIngestQueue(
                queuedClient,
                awaiter,
                concurrentIngestionCount);

            return ingestQueue;
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