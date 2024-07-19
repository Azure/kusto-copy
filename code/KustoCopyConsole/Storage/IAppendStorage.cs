using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IAppendStorage
    {
        /// <summary>Returns the full content of the storage.</summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<byte[]> LoadAllAsync(CancellationToken ct);
     
        /// <summary>Replace atomically the content of the storage.</summary>
        /// <param name="buffer"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task AtomicReplaceAsync(byte[] buffer, CancellationToken ct);

        /// <summary>Attempt to append the buffer to storage.</summary>
        /// <param name="buffer"></param>
        /// <param name="ct"></param>
        /// <returns>
        /// <c>true</c> if append succeeds.
        /// <c>false</c> if storage requires compaction.
        /// </returns>
        Task<bool> AtomicAppendAsync(byte[] buffer, CancellationToken ct);
    }
}