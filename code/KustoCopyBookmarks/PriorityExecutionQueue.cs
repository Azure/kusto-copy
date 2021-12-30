using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
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

        public PriorityExecutionQueue(int parallelRunCount, IComparer<TPriority> priorityComparer)
        {
            _parallelRunCount = parallelRunCount;
            _requestQueue = new PriorityQueue<Request, TPriority>(priorityComparer);
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

        public async Task RequestRunAsync(TPriority priority, Func<Task> actionAsync)
        {
            var request = new Request();

            _requestQueue.Enqueue(request, priority);
            PumpRequestOut();

            await request.Source.Task;

            try
            {
                await actionAsync();
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