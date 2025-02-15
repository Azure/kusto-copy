using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IAppendStorage2
    {
        /// <summary>Maximum size of buffer that can be written.</summary>
        int MaxBufferSize { get; }

        /// <summary>Maximum number of blocks that can be appended on a blob.</summary>
        int? MaxAppendBlock { get; }

        /// <summary>Open a reading stream.</summary>
        /// <param name="path"></param>
        /// <returns><c>null</c> if no blob exists.</returns>
        Stream? OpenReadAsync(string path);

        /// <summary>Attempt to append the buffer to storage.</summary>
        /// <param name="path"></param>
        /// <param name="buffer"></param>
        /// <param name="ct"></param>
        Task AtomicAppendAsync(string path, byte[] buffer, CancellationToken ct);

        /// <summary>Moves a blob from one path to another.</summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        /// <returns></returns>
        Task Move(string source, string destination);
    }
}