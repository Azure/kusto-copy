using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class ProgressBar : IAsyncDisposable
    {
        private readonly TaskCompletionSource _timerCompletionSource = new();
        private readonly Task _timerTask;
        private bool _isCompleted = false;

        public ProgressBar(TimeSpan refreshRate, Func<ProgressReport> progressFunc)
        {
            _timerTask = MonitorAsync(refreshRate, progressFunc);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _timerCompletionSource.TrySetResult();
            await _timerTask;
        }

        private async Task MonitorAsync(TimeSpan refreshRate, Func<ProgressReport> progressFunc)
        {
            while (!_timerCompletionSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    Task.Delay(refreshRate),
                    _timerCompletionSource.Task);

                if (!_timerCompletionSource.Task.IsCompleted)
                {
                    var report = progressFunc();

                    switch (report.ProgessStatus)
                    {
                        case ProgessStatus.Nothing:
                            break;
                        case ProgessStatus.Progress:
                            Console.WriteLine($"{report.Text} (progress)");
                            break;
                        case ProgessStatus.Completed:
                            _isCompleted = true;
                            _timerCompletionSource.TrySetResult();
                            break;

                        default:
                            throw new NotSupportedException(
                                $"{nameof(ProgessStatus)} with {report.ProgessStatus}");
                    }
                }
            }
            if (_isCompleted)
            {
                var report = progressFunc();

                Console.WriteLine($"{report.Text} (completed)");
            }
        }
    }
}