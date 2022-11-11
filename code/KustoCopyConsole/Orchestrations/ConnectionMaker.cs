﻿using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Kusto.Data;
using KustoCopyConsole.KustoQuery;
using KustoCopyConsole.Parameters;

namespace KustoCopyConsole.Orchestrations
{
    public class ConnectionMaker
    {
        #region Constructors
        public static ConnectionMaker Create(MainParameterization parameterization)
        {
            var lakeFolderBuilder =
                new KustoConnectionStringBuilder(parameterization.LakeFolderConnectionString);
            var credentials = CreateCredentials(lakeFolderBuilder);
            var lakeFolderUri = new Uri(lakeFolderBuilder.DataSource);
            var lakeFolderClient = new DataLakeDirectoryClient(lakeFolderUri, credentials);
            var lakeFolderBlobClient = new BlobClient(lakeFolderUri, credentials);
            var sourceQueuedClient = parameterization.Source != null
                ? CreateKustoQueuedClient(
                    parameterization.Source!.ClusterQueryConnectionString!,
                    parameterization.Source!.ConcurrentQueryCount)
                : null;
            var destinationQueuedClient = parameterization.Destination != null
                ? CreateKustoQueuedClient(
                    parameterization.Destination!.ClusterQueryConnectionString!,
                    parameterization.Destination!.ConcurrentQueryCount)
                : null;

            return new ConnectionMaker(
                lakeFolderClient,
                lakeFolderBlobClient,
                sourceQueuedClient,
                destinationQueuedClient);
        }

        private static KustoQueuedClient CreateKustoQueuedClient(
            string clusterQueryConnectionString,
            int concurrentQueryCount)
        {
            var sourceBuilder = new KustoConnectionStringBuilder(clusterQueryConnectionString);
            var sourceKustoClient = new KustoClient(NormalizeBuilder(sourceBuilder));
            var sourceQueuedClient = new KustoQueuedClient(
                sourceKustoClient,
                concurrentQueryCount,
                0);

            return sourceQueuedClient;
        }

        private static KustoConnectionStringBuilder NormalizeBuilder(
            KustoConnectionStringBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(builder.ApplicationClientId))
            {
                return builder.WithAadAzureTokenCredentialsAuthentication(
                    new DefaultAzureCredential());
            }
            else
            {
                return builder;
            }
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

        private ConnectionMaker(
            DataLakeDirectoryClient lakeFolderClient,
            BlobClient lakeFolderBlobClient,
            KustoQueuedClient? sourceQueuedClient,
            KustoQueuedClient? destinationQueuedClient)
        {
            LakeFolderClient = lakeFolderClient;
            LakeFolderBlobClient = lakeFolderBlobClient;
            SourceQueuedClient = sourceQueuedClient;
            DestinationQueuedClient = destinationQueuedClient;
        }
        #endregion

        public DataLakeDirectoryClient LakeFolderClient { get; }

        public BlobClient LakeFolderBlobClient { get; }

        public KustoQueuedClient? SourceQueuedClient { get; }

        public KustoQueuedClient? DestinationQueuedClient { get; }
    }
}