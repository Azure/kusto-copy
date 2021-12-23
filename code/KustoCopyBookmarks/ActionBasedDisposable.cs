using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBookmarks
{
    internal class ActionBasedDisposable : IDisposable
    {
        private readonly Action _disposeAction;
        private bool _isDisposed = false;

        public ActionBasedDisposable(Action disposeAction)
        {
            _disposeAction = disposeAction;
        }

        void IDisposable.Dispose()
        {
            if(_isDisposed)
            {
                throw new InvalidOperationException("Object has already been disposed");
            }

            _disposeAction();
            _isDisposed = true;
        }
    }
}