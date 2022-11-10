using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    public class SingletonExecution
    {
        private volatile TaskCompletionSource _source = new TaskCompletionSource();

        public SingletonExecution()
        {
            _source.SetResult();
        }

        /// <summary>
        /// Wait for "current" execution to complete (if any are ongoing).
        /// If the caller can be the next execution, it executes the action, otherwise returns.
        /// </summary>
        /// <param name="asyncAction"></param>
        /// <returns></returns>
        public async Task SingleRunAsync(Func<Task> asyncAction)
        {
            var oldSource = _source;

            if (_source.Task.Status != TaskStatus.RanToCompletion)
            {
                await _source.Task;
            }
            else
            {
                var newSource = new TaskCompletionSource();
                var alternateSource =
                    Interlocked.CompareExchange(ref _source, newSource, oldSource);

                if (alternateSource == oldSource)
                {   //  Do the single execution
                    await asyncAction();
                    //  Release other threads
                    newSource.SetResult();
                }
                else
                {   //  Some other thread won, just wait and return
                    await alternateSource.Task;
                }
            }
        }
    }
}