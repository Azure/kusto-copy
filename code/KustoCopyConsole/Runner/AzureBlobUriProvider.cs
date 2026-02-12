using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Sas;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity.Keys;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class AzureBlobUriProvider
    {
        #region Inner Types
        private class DirectoryProvider
        {
            private readonly DataLakeDirectoryClient _directoryClient;
            private readonly BlobContainerClient _containerClient;
            private readonly AsyncCache<UserDelegationKey> _keyCache;

            public DirectoryProvider(Uri rootFolderUri, TokenCredential credential)
            {
                _directoryClient = new DataLakeDirectoryClient(rootFolderUri, credential);
                _containerClient = new BlobContainerClient(rootFolderUri, credential);
                _keyCache = new(FetchUserDelegationKey);
            }

            public Uri RootDirectoryUri => _directoryClient.Uri;

            public async Task TestAuthenticationAsync(CancellationToken ct)
            {
                await _containerClient.ExistsAsync();
            }

            public async Task<Uri> GetWritableFolderUrisAsync(string subPath, CancellationToken ct)
            {
                var userDelegationKey = await _keyCache.GetCacheItemAsync(ct);
                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = _containerClient.Name,
                    Resource = "c", // "b" for blob, "c" for container
                    ExpiresOn = DateTimeOffset.UtcNow.Add(WRITE_TIME_OUT)
                };

                // Add permissions (e.g., read and write)
                sasBuilder.SetPermissions(BlobSasPermissions.Write);

                // Generate SAS token
                var sasToken = sasBuilder.ToSasQueryParameters(
                    userDelegationKey,
                    _containerClient.AccountName).ToString();
                var subDirectoryClient = _directoryClient.GetSubDirectoryClient(subPath);
                var uriBuilder = new UriBuilder(subDirectoryClient.Uri)
                {
                    Query = sasToken
                };

                return uriBuilder.Uri;
            }

            public async Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct)
            {
                var userDelegationKey = await _keyCache.GetCacheItemAsync(ct);
                var sasBuilder = new BlobSasBuilder
                {
                    BlobContainerName = _containerClient.Name,
                    Resource = "b", // "b" for blob, "c" for container
                    ExpiresOn = DateTimeOffset.UtcNow.Add(READ_TIME_OUT)
                };

                // Add permissions (e.g., read and write)
                sasBuilder.SetPermissions(BlobSasPermissions.Read);

                // Generate SAS token
                var sasToken = sasBuilder.ToSasQueryParameters(
                    userDelegationKey,
                    _containerClient.AccountName).ToString();
                var uriBuilder = new UriBuilder(uri)
                {
                    Query = sasToken
                };

                return uriBuilder.Uri;
            }

            public async Task DeleteStagingDirectoryAsync(string subDirectory, CancellationToken ct)
            {
                var subDirectoryClient = _directoryClient.GetSubDirectoryClient(subDirectory);

                await subDirectoryClient.DeleteIfExistsAsync();
            }

            private async Task<(TimeSpan, UserDelegationKey)> FetchUserDelegationKey()
            {
                var refreshPeriod = READ_TIME_OUT;
                var tolerance = TimeSpan.FromSeconds(30);
                var key = await _containerClient.GetParentBlobServiceClient().GetUserDelegationKeyAsync(
                    DateTimeOffset.UtcNow,
                    DateTimeOffset.UtcNow.Add(refreshPeriod));

                return (refreshPeriod.Subtract(tolerance), key.Value);
            }
        }
        #endregion

        private static readonly TimeSpan READ_TIME_OUT = TimeSpan.FromDays(5);
        private static readonly TimeSpan WRITE_TIME_OUT = TimeSpan.FromMinutes(90);

        private readonly IImmutableDictionary<string, DirectoryProvider> _providerMap;

        public AzureBlobUriProvider(
            IEnumerable<Uri> stagingStorageDirectories,
            TokenCredential credential)
        {
            _providerMap = stagingStorageDirectories
                .Select(u => new DirectoryProvider(u, credential))
                .ToImmutableDictionary(p => p.RootDirectoryUri.ToString(), p => p);

            if (_providerMap.Count == 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(stagingStorageDirectories),
                    "No staging directory provided");
            }
        }

        public async Task TestAuthenticationAsync(CancellationToken ct)
        {
            await _providerMap.First().Value.TestAuthenticationAsync(ct);
        }

        public async Task<IEnumerable<Uri>> GetWritableFolderUrisAsync(
            BlockKey blockKey,
            CancellationToken ct)
        {
            var subDirectory = GetSubDirectory(blockKey);
            var tasks = _providerMap.Values
                .Select(c => c.GetWritableFolderUrisAsync(subDirectory, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);

            var uris = tasks
                .Select(t => t.Result)
                .ToImmutableArray();

            return uris;
        }

        public async Task<Uri> AuthorizeUriAsync(Uri uri, CancellationToken ct)
        {
            var storageRoot = _providerMap.Keys
                .Where(k => uri.ToString().StartsWith(k))
                .FirstOrDefault();

            if (storageRoot != null)
            {
                var provider = _providerMap[storageRoot];

                return await provider.AuthorizeUriAsync(uri, ct);
            }
            else
            {
                throw new CopyException($"Uri isn't from staging directories:  '{uri}'", false);
            }
        }

        public async Task DeleteStagingRootDirectoryAsync(
            IterationKey iterationKey,
            CancellationToken ct)
        {
            var subDirectory = GetSubDirectory(iterationKey);
            var tasks = _providerMap.Values
                .Select(c => c.DeleteStagingDirectoryAsync(subDirectory, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);
        }

        public async Task DeleteStagingDirectoryAsync(BlockKey blockKey, CancellationToken ct)
        {
            var subDirectory = GetSubDirectory(blockKey);
            var tasks = _providerMap.Values
                .Select(c => c.DeleteStagingDirectoryAsync(subDirectory, ct))
                .ToImmutableArray();

            await TaskHelper.WhenAllWithErrors(tasks);
        }

        private string GetSubDirectory(IterationKey iterationKey)
        {
            var subDirectoryPath = $"activities/{iterationKey.ActivityName}/" +
                $"iterations/{iterationKey.IterationId:D20}";

            return subDirectoryPath;
        }

        private string GetSubDirectory(BlockKey blockKey)
        {
            var subDirectoryPath =
                $"{GetSubDirectory(blockKey.IterationKey)}/blocks/{blockKey.BlockId:D20}";

            return subDirectoryPath;
        }
    }
}