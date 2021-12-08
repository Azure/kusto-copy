using Azure;
using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using KustoCopyBookmarks;
using System.Timers;

namespace kusto_copy
{
    internal class BlobLock : IAsyncDisposable
    {
        private static readonly TimeSpan DEFAULT_LEASE_DURATION = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DEFAULT_LEASE_RENEWAL_PERIOD = TimeSpan.FromSeconds(40);

        private readonly BlobLeaseClient _leaseClient;
        private readonly System.Timers.Timer _timer;

        private BlobLock(BlobLeaseClient leaseClient)
        {
            _leaseClient = leaseClient;
            _timer = new System.Timers.Timer(DEFAULT_LEASE_RENEWAL_PERIOD.TotalMilliseconds);
            _timer.Elapsed += OnElapsed;
            _timer.Enabled = true;
        }

        internal static async Task<IAsyncDisposable?> CreateAsync(BlobClient blobClient)
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
            await _leaseClient.ReleaseAsync();
        }

        private void OnElapsed(object? sender, ElapsedEventArgs e)
        {
            var task = _leaseClient.RenewAsync();

            task.Wait();
        }
    }
}