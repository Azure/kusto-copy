using Azure.Storage.Files.DataLake;

namespace KustoCopyServices
{
    public class TempFolderService : ITempFolderService
    {
        private readonly DataLakeDirectoryClient _tempFolderClient;

        private TempFolderService(DataLakeDirectoryClient tempFolderClient)
        {
            _tempFolderClient = tempFolderClient;
        }

        public static async Task<TempFolderService> CreateAsync(
            DataLakeDirectoryClient folderClient)
        {
            var tempFolderClient = folderClient.GetSubDirectoryClient("temp");

            await tempFolderClient.CreateIfNotExistsAsync();

            return new TempFolderService(tempFolderClient);
        }
    }
}