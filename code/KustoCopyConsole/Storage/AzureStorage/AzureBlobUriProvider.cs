using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using KustoCopyConsole.Concurrency;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class AzureBlobUriProvider : IStagingBlobUriProvider
    {
        #region Inner Types
        private class ContainerProvider
        {
            private readonly BlobContainerClient _client;
            private readonly AsyncCache<UserDelegationKey> _keyCache;

            public ContainerProvider(Uri containerUri, TokenCredential credential)
            {
                _client = new BlobContainerClient(containerUri, credential);
                _keyCache = new(FetchUserDelegationKey);
            }

            public Uri ContainerUri => _client.Uri;

            public async Task<Uri> GetWritableRootUrisAsync(string path, CancellationToken ct)
            {
                var userDelegationKey = await _keyCache.GetCacheItemAsync(ct);
                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = _client.Name,
                    BlobName = path,
                    Resource = "c", // "b" for blob, "c" for container
                    ExpiresOn = DateTimeOffset.UtcNow.Add(WRITE_TIME_OUT)
                };

                // Add permissions (e.g., read and write)
                sasBuilder.SetPermissions(BlobSasPermissions.Write);

                // Generate SAS token
                var sasToken = sasBuilder.ToSasQueryParameters(
                    userDelegationKey,
                    _client.AccountName).ToString();
                var uri = new Uri($"{ContainerUri}/{path}?{sasToken}");

                return uri;
            }

            private async Task<(TimeSpan, UserDelegationKey)> FetchUserDelegationKey()
            {
                //DateTimeOffset.UtcNow.Add(READ_TIME_OUT));
                var refreshPeriod = TimeSpan.FromSeconds(20);
                var tolerance = TimeSpan.FromSeconds(10);
                var key = await _client.GetParentBlobServiceClient().GetUserDelegationKeyAsync(
                    DateTimeOffset.UtcNow,
                    DateTimeOffset.UtcNow.Add(refreshPeriod));

                return (refreshPeriod.Subtract(tolerance), key.Value);
            }
        }
        #endregion

        private static readonly TimeSpan READ_TIME_OUT = TimeSpan.FromDays(5);
        private static readonly TimeSpan WRITE_TIME_OUT = TimeSpan.FromMinutes(90);

        private readonly IImmutableDictionary<string, ContainerProvider> _containerMap;

        public AzureBlobUriProvider(
            IImmutableList<Uri> stagingStorageContainers,
            TokenCredential credential)
        {
            _containerMap = stagingStorageContainers
                .Select(u => new ContainerProvider(u, credential))
                .ToImmutableDictionary(p => p.ContainerUri.ToString(), p => p);
        }

        async Task<IEnumerable<Uri>> IStagingBlobUriProvider.GetWritableRootUrisAsync(string path, CancellationToken ct)
        {
            var tasks = _containerMap.Values
                .Select(c => c.GetWritableRootUrisAsync(path, ct))
                .ToImmutableArray();

            await Task.WhenAll(tasks);

            var uris = tasks
                .Select(t=>t.Result)
                .ToImmutableArray();

            return uris;
        }

        Task<Uri> IStagingBlobUriProvider.AuthorizeUriAsync(Uri uri, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}