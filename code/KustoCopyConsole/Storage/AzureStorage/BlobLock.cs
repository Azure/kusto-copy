using Azure;
using Azure.Storage.Blobs.Specialized;
using System.Threading;

namespace KustoCopyConsole.Storage.AzureStorage
{
    internal class BlobLock : IAsyncDisposable
    {
#if DEBUG
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(20);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(10);
#else
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(40);
#endif

        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _backgroundCompletedSource = new();

        #region Constructors
        private BlobLock(BlobLeaseClient leaseClient, CancellationToken ct)
        {
            LeaseClient = leaseClient;
            _backgroundTask = Task.Run(() => BackGroundRenewLockAsync(ct));
        }

        internal static async Task<BlobLock> CreateAsync(
            BlobBaseClient blobClient,
            CancellationToken ct)
        {
            var leaseClient = blobClient.GetBlobLeaseClient();

            try
            {
                await leaseClient.AcquireAsync(DEFAULT_LEASE_DURATION);

                return new BlobLock(leaseClient, ct);
            }
            catch (RequestFailedException ex)
            {
                if (ex.ErrorCode == "LeaseAlreadyPresent")
                {
                    throw new CopyException("Lease on blob already present", false);
                }
                else
                {
                    throw;
                }
            }
        }
        #endregion

        public BlobLeaseClient LeaseClient { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _backgroundCompletedSource.SetResult();
            await _backgroundTask;
        }

        private async Task BackGroundRenewLockAsync(CancellationToken ct)
        {
            while (!_backgroundCompletedSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    Task.Delay(DEFAULT_LEASE_RENEWAL_PERIOD, ct),
                    _backgroundCompletedSource.Task);
                await LeaseClient.RenewAsync(null, ct);
            }
        }
    }
}