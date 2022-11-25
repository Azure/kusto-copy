using Azure;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using System.Threading;
using System.Timers;

namespace KustoCopyConsole.Storage
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

        private readonly BlobLeaseClient _leaseClient;
        private readonly System.Timers.Timer _timer;

        private BlobLock(BlobLeaseClient leaseClient)
        {
            _leaseClient = leaseClient;
            _timer = new System.Timers.Timer(DEFAULT_LEASE_RENEWAL_PERIOD.TotalMilliseconds);
            _timer.Elapsed += OnElapsed;
            _timer.Enabled = true;
        }

        internal static async Task<IAsyncDisposable?> CreateAsync(BlobBaseClient blobClient)
        {
            var leaseClient = blobClient.GetBlobLeaseClient();

            try
            {
                await leaseClient.AcquireAsync(DEFAULT_LEASE_DURATION);

                return new BlobLock(leaseClient);
            }
            catch (RequestFailedException ex)
            {
                if (ex.ErrorCode == "LeaseAlreadyPresent")
                {
                    return null;
                }
                else
                {
                    throw;
                }
            }
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _timer.Dispose();
            await _leaseClient.ReleaseAsync();
        }

        private void OnElapsed(object? sender, ElapsedEventArgs e)
        {
            var task = _leaseClient.RenewAsync();

            task.Wait();
        }
    }
}