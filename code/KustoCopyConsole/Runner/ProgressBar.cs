using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class ProgressBar : IAsyncDisposable
    {
        private readonly TaskCompletionSource _timerCompletionSource = new();
        private readonly Task _timerTask;
        private bool _isCompleted = false;

        public ProgressBar(TimeSpan refreshRate, Func<string> progressFunc)
        {
            _timerTask = MonitorAsync(refreshRate, progressFunc);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _timerCompletionSource.TrySetResult();
            await _timerTask;
        }

        public async Task CompleteAsync()
        {
            _isCompleted = true;
            _timerCompletionSource.SetResult();
            await _timerTask;
        }

        private async Task MonitorAsync(TimeSpan refreshRate, Func<string> progressFunc)
        {
            while (!_timerCompletionSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    Task.Delay(refreshRate),
                    _timerCompletionSource.Task);

                if (!_timerCompletionSource.Task.IsCompleted)
                {
                    var text = progressFunc();

                    Console.WriteLine($"{text} (progress)");
                }
            }
            if (_isCompleted)
            {
                var text = progressFunc();

                Console.WriteLine($"{text} (completed)");
            }
        }
    }
}