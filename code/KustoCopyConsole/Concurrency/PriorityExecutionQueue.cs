using Polly;
using Polly.Bulkhead;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    public class PriorityExecutionQueue<TPriority>
    {
        #region Inner Types
        private abstract class Request
        {
            public abstract Task ExecuteAsync();
        }

        private class Request<T> : Request
        {
            private readonly Func<Task<T>> _asyncAction;

            public Request(Func<Task<T>> asyncAction)
            {
                _asyncAction = asyncAction;
            }

            public TaskCompletionSource<T> Source { get; } = new TaskCompletionSource<T>();

            public override async Task ExecuteAsync()
            {
                var value = await _asyncAction();

                Source.SetResult(value);
            }
        }
        #endregion

        private readonly AsyncBulkheadPolicy _bulkheadPolicy;
        private readonly PriorityQueue<Request, TPriority> _requestQueue;

        public PriorityExecutionQueue(int parallelRunCount)
        {
            _bulkheadPolicy = Policy.BulkheadAsync(parallelRunCount, int.MaxValue);
            _requestQueue = new PriorityQueue<Request, TPriority>();
        }

        public int ParallelRunCount => _bulkheadPolicy.BulkheadAvailableCount;

        public async Task<T> RequestRunAsync<T>(TPriority priority, Func<Task<T>> actionAsync)
        {
            var request = new Request<T>(actionAsync);

            lock (_requestQueue)
            {   //  Add our item in the queue
                _requestQueue.Enqueue(request, priority);
            }
            //  Remove/execute one item from the queue (when parallelism allows)
            //  Either the item is "us" or someone before us
            await _bulkheadPolicy.ExecuteAsync(async () =>
            {
                Request? request;

                lock (_requestQueue)
                {
                    if (!_requestQueue.TryDequeue(out request, out _))
                    {
                        throw new InvalidOperationException("Request queue is corrupted");
                    }
                }

                await request.ExecuteAsync();
            });

            //  Wait for our own turn
            return await request.Source.Task;
        }

        public async Task RequestRunAsync(TPriority priority, Func<Task> actionAsync)
        {
            await RequestRunAsync(priority, async () =>
            {
                await actionAsync();

                return 0;
            });
        }
    }
}