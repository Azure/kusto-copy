using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    public class StopGoAwaiter
    {
        private TaskCompletionSource _source = new TaskCompletionSource();

        public StopGoAwaiter(bool isStopped)
        {
            if (!isStopped)
            {
                _source.TrySetResult();
            }
        }

        public bool IsStopped => !_source.Task.IsCompleted;

        public Task WaitForGoAsync()
        {
            return _source.Task;
        }

        public void Stop()
        {
            if (!IsStopped)
            {
                _source = new TaskCompletionSource();
            }
        }

        public void Go()
        {
            if (IsStopped)
            {
                _source.TrySetResult();
            }
        }
    }
}