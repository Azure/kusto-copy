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
                    Resource = "c", // "b" for blob, "c" for container
                    ExpiresOn = DateTimeOffset.UtcNow.Add(WRITE_TIME_OUT)
                };

                // Add permissions (e.g., read and write)
                sasBuilder.SetPermissions(BlobSasPermissions.Write | BlobSasPermissions.Read | BlobSasPermissions.List);

                // Generate SAS token
                var sasToken = sasBuilder.ToSasQueryParameters(
                    userDelegationKey,
                    _client.AccountName).ToString();
                var uriBuilder = new UriBuilder(ContainerUri.ToString())
                {
                    Path = $"{_client.Name}/{path}",
                    Query = sasToken
                };
                var uri = uriBuilder.Uri;

                return uri;
            }

            private async Task<(TimeSpan, UserDelegationKey)> FetchUserDelegationKey()
            {
                var refreshPeriod = READ_TIME_OUT;
                var tolerance = TimeSpan.FromSeconds(30);
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

        async Task<IEnumerable<Uri>> IStagingBlobUriProvider.GetWritableFolderUrisAsync(string path, CancellationToken ct)
        {
            var tasks = _containerMap.Values
                .Select(c => c.GetWritableRootUrisAsync(path, ct))
                .ToImmutableArray();

            await Task.WhenAll(tasks);

            var uris = tasks
                .Select(t => t.Result)
                .ToImmutableArray();

            return uris;
        }

        Task<Uri> IStagingBlobUriProvider.AuthorizeUriAsync(Uri uri, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}