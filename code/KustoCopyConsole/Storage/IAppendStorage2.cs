namespace KustoCopyConsole.Storage
{
    public interface IAppendStorage2
    {
        /// <summary>Attempt to append the buffer to storage.</summary>
        /// <param name="buffer"></param>
        /// <param name="ct"></param>
        /// <returns><c>false</c> iif blob is full.</returns>
        Task<bool> AtomicAppendAsync(byte[] buffer, CancellationToken ct);
    }
}