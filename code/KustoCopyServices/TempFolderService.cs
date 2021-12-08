using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Exceptions;

namespace KustoCopyServices
{
    public class TempFolderService : ITempFolderService
    {
        private readonly DataLakeDirectoryClient _tempFolderClient;
        private readonly TokenCredential _tokenCredential;

        private TempFolderService(
            DataLakeDirectoryClient tempFolderClient,
            TokenCredential tokenCredential)
        {
            _tempFolderClient = tempFolderClient;
            _tokenCredential = tokenCredential;
        }

        public static async Task<TempFolderService> CreateAsync(
            DataLakeDirectoryClient folderClient,
            TokenCredential tokenCredential)
        {
            var tempFolderClient = folderClient.GetSubDirectoryClient("temp");

            //await tempFolderClient.CreateIfNotExistsAsync();
            await ValueTask.CompletedTask;

            return new TempFolderService(tempFolderClient, tokenCredential);
        }

        public async Task RunAsync()
        {
            await Task.CompletedTask;
        }

        #region ITempFolderService
        TokenCredential ITempFolderService.Credential => _tokenCredential;

        DataLakeDirectoryClient ITempFolderService.GetTempFolder()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}