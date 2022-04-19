using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyFoundation.Concurrency
{
    public class PriorityExecutionQueue<TPriority>
    {
        #region Inner Types
        private class Request
        {
            public TaskCompletionSource Source { get; } = new TaskCompletionSource();

        }
        #endregion

        private readonly int _parallelRunCount;
        private readonly PriorityQueue<Request, TPriority> _requestQueue;
        private volatile int _availableRunningSlots;

        public PriorityExecutionQueue(int parallelRunCount)
        {
            _parallelRunCount = parallelRunCount;
            _requestQueue = new PriorityQueue<Request, TPriority>();
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

        public async Task<T> RequestRunAsync<T>(TPriority priority, Func<Task<T>> actionAsync)
        {
            var request = new Request();

            lock (_requestQueue)
            {
                _requestQueue.Enqueue(request, priority);
            }
            PumpRequestOut();

            await request.Source.Task;

            try
            {
                return await actionAsync();
            }
            finally
            {
                lock (_requestQueue)
                {
                    //  Returning the slot as the request is over
                    ++_availableRunningSlots;
                    PumpRequestOut();
                }
            }
        }

        public async Task RequestRunAsync(TPriority priority, Func<Task> actionAsync)
        {
            await RequestRunAsync(priority, async () =>
            {
                await actionAsync();

                return 0;
            });
        }

        private void PumpRequestOut()
        {
            //  Priority queue aren't thread safe
            lock (_requestQueue)
            {
                while (_requestQueue.Count > 0 && _availableRunningSlots > 0)
                {
                    Request? request;

                    --_availableRunningSlots;
                    if (_requestQueue.TryDequeue(out request, out _))
                    {
                        request.Source.SetResult();
                        //  Keep pumping
                    }
                }
            }
        }
    }
}