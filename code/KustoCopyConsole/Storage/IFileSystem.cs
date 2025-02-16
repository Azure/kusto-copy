using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IFileSystem
    {
        /// <summary>Maximum size of buffer that can be written.</summary>
        int MaxBufferSize { get; }

        /// <summary>Opens a reading stream.</summary>
        /// <param name="path"></param>
        /// <param name="ct"></param>
        /// <returns><c>null</c> if no blob exists.</returns>
        Task<Stream?> OpenReadAsync(string path, CancellationToken ct);

        /// <summary>Opens for writing.</summary>
        /// <param name="path"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<IAppendStorage2> OpenWriteAsync(string path, CancellationToken ct);

        /// <summary>Moves a blob from one path to another.</summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task MoveAsync(string source, string destination, CancellationToken ct);

        /// <summary>Removes a folder with all blobs inside.</summary>
        /// <param name="path"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveFolderAsync(string path, CancellationToken ct);
    }
}