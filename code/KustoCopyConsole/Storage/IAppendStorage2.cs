﻿namespace KustoCopyConsole.Storage
{
    public interface IAppendStorage2
    {
        /// <summary>Attempt to append the content to storage.</summary>
        /// <param name="content"></param>
        /// <param name="ct"></param>
        /// <returns><c>false</c> iif blob is full, i.e. the next append would fail.</returns>
        Task<bool> AtomicAppendAsync(IEnumerable<byte> content, CancellationToken ct);
    }
}