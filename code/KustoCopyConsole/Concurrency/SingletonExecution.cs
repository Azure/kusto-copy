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
        /// If the caller can be the next execution, it executes the action,
        /// otherwise it returns after the current executed.
        /// </summary>
        /// <param name="asyncAction"></param>
        /// <returns>
        /// <c>true</c> iif the action passed was executed, <c>false</c> otherwise.
        /// </returns>
        public async Task<bool> SingleRunAsync(Func<Task> asyncAction)
        {
            var oldSource = _source;

            if (_source.Task.Status != TaskStatus.RanToCompletion)
            {
                await _source.Task;

                return false;
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

                    return true;
                }
                else
                {   //  Some other thread won, just wait and return
                    await alternateSource.Task;

                    return false;
                }
            }
        }
    }
}