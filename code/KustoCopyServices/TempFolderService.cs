﻿using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Exceptions;
using KustoCopyBookmarks;
using System.Collections.Immutable;

namespace KustoCopyServices
{
    public class TempFolderService
    {
        #region Inner Types
        public class TempFolderLease : ITempFolderLease
        {
            private readonly DataLakeDirectoryClient _client;
            private readonly Action _disposeAction;

            public TempFolderLease(DataLakeDirectoryClient client, Action disposeAction)
            {
                _client = client;
                _disposeAction = disposeAction;
            }

            #region ITempFolderLease
            DataLakeDirectoryClient ITempFolderLease.Client => _client;

            void IDisposable.Dispose()
            {
                _disposeAction();
            }
            #endregion
        }
        #endregion

        private readonly DataLakeDirectoryClient _tempFolderClient;
        private readonly TokenCredential _tokenCredential;
        private volatile int _currentMainFolder;

        private TempFolderService(
            DataLakeDirectoryClient tempFolderClient,
            TokenCredential tokenCredential,
            IImmutableList<int> subFolders)
        {
            _tempFolderClient = tempFolderClient;
            _tokenCredential = tokenCredential;
            _currentMainFolder = subFolders.Any()
                ? subFolders.Max() + 1
                : 1;
        }

        public static async Task<TempFolderService> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential tokenCredential)
        {
            var tempFolderClient = folderClient.GetSubDirectoryClient("temp");

            await tempFolderClient.CreateIfNotExistsAsync();

            var paths = await tempFolderClient.GetPathsAsync().ToListAsync();
            var subFolders = paths
                .Where(p => p.IsDirectory == true)
                .Select(p => ParseFolderName(p.Name))
                .ToImmutableArray();

            return new TempFolderService(tempFolderClient, tokenCredential, subFolders);
        }

        public async Task RunAsync()
        {
            await Task.CompletedTask;
        }

        public TokenCredential Credential => _tokenCredential;

        public ITempFolderLease LeaseTempFolder()
        {
            var tempFolderClient = _tempFolderClient
                .GetSubDirectoryClient(_currentMainFolder.ToString())
                .GetSubDirectoryClient(Guid.NewGuid().ToString());
            var tempFolderLease = new TempFolderLease(tempFolderClient, () => { });

            return tempFolderLease;
        }

        private static int ParseFolderName(string path)
        {
            var folderName = Path.GetFileName(path);
            int result;

            if (!int.TryParse(folderName, out result))
            {
                throw new InvalidOperationException($"Folder '{folderName}' in temp folder");
            }

            return result;
        }
    }
}