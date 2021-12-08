using Azure.Core;
using Azure.Storage.Files.DataLake;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    public interface ITempFolderService
    {
        TokenCredential Credential { get; }

        DataLakeDirectoryClient GetTempFolder();
    }
}