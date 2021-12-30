using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    public class ExecutionQueue
    {
        #region Inner Types
        private class Request
        {
            public TaskCompletionSource Source { get; } = new TaskCompletionSource();

        }
        #endregion

        private readonly int _parallelRunCount;
        private readonly ConcurrentQueue<Request> _requestQueue = new ConcurrentQueue<Request>();
        private volatile int _availableRunningSlots;

        public ExecutionQueue(int parallelRunCount)
        {
            _parallelRunCount = parallelRunCount;
            _availableRunningSlots = _parallelRunCount;
        }

        public int ParallelRunCount
        {
            get { return _parallelRunCount; }
            set
            {
                throw new NotImplementedException();
            }
        }

        public async Task RequestRunAsync(Func<Task> actionAsync)
        {
            await RequestRunAsync(async () =>
            {
                await actionAsync();

                return 0;
            });
        }

        public async Task<T> RequestRunAsync<T>(Func<Task<T>> actionAsync)
        {
            var request = new Request();

            _requestQueue.Enqueue(request);
            PumpRequestOut();

            await request.Source.Task;

            try
            {
                return await actionAsync();
            }
            finally
            {
                //  Returning the slot as the request is over
                Interlocked.Increment(ref _availableRunningSlots);
                PumpRequestOut();
            }
        }

        private void PumpRequestOut()
        {
            while (_requestQueue.Any())
            {
                //  We try to get a slot for running
                var slot = Interlocked.Decrement(ref _availableRunningSlots);

                if (slot >= 0)
                {   //  We got a valid slot
                    Request? request;

                    if (_requestQueue.TryDequeue(out request))
                    {
                        request.Source.SetResult();
                        //  Keep pumping
                    }
                    else
                    {   //  Return the slot (will still retry the queue in case of racing conditions)
                        Interlocked.Increment(ref _availableRunningSlots);
                    }
                }
                else
                {   //  We give the slot back
                    var afterSlot = Interlocked.Increment(ref _availableRunningSlots);

                    //  There might be racing condition where a slot became available in the meantime
                    //  If not, we terminate
                    if (afterSlot <= 0)
                    {
                        return;
                    }
                }
            }
        }
    }
}