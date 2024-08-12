using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    internal class AsyncLock
    {
        #region Inner Types
        private class DisposableLock : IDisposable
        {
            private readonly AsyncLock _asyncLock;

            public DisposableLock(AsyncLock asyncLock)
            {
                _asyncLock = asyncLock;
            }

            void IDisposable.Dispose()
            {
                _asyncLock.ReleaseLock();
            }
        }
        #endregion

        private static readonly int TRUE_VALUE = Convert.ToInt32(true);
        private static readonly int FALSE_VALUE = Convert.ToInt32(false);

        private volatile int _lockValue = FALSE_VALUE;

        public IDisposable? TryGetLock()
        {
            if (Interlocked.CompareExchange(ref _lockValue, TRUE_VALUE, FALSE_VALUE)
                == FALSE_VALUE)
            {
                return new DisposableLock(this);
            }
            else
            {
                return null;
            }
        }

        private void ReleaseLock()
        {
            if (Interlocked.CompareExchange(ref _lockValue, FALSE_VALUE, TRUE_VALUE)
                == TRUE_VALUE)
            {   //  Success
            }
            else
            {
                throw new InvalidOperationException("Async lock was already released");
            }
        }
    }
}