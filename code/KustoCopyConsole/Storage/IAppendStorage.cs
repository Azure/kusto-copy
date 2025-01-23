using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal interface IAppendStorage : IAsyncDisposable
    {
        /// <summary>Maximum size of buffer that can be written.</summary>
        int MaxBufferSize { get; }

        /// <summary>
        /// Returns <c>true</c> iif compaction is required before invoking
        /// <see cref="AtomicAppendAsync(byte[], CancellationToken)"/>.
        /// </summary>
        bool IsCompactionRequired { get; }

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
        Task AtomicAppendAsync(byte[] buffer, CancellationToken ct);
    }
}