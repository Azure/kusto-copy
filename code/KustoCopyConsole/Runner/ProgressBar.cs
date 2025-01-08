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
            var isCancel = true;

            while (!_timerCompletionSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    Task.Delay(refreshRate),
                    _timerCompletionSource.Task);

                if (!_timerCompletionSource.Task.IsCompleted)
                {
                    if (!ProcessReport(progressFunc))
                    {
                        _timerCompletionSource.TrySetResult();
                        isCancel = false;
                    }
                }
            }
            if (isCancel)
            {
                ProcessReport(progressFunc);
            }
        }

        private bool ProcessReport(Func<ProgressReport> progressFunc)
        {
            var report = progressFunc();

            switch (report.ProgessStatus)
            {
                case ProgessStatus.Nothing:
                    return true;
                case ProgessStatus.Progress:
                    Console.WriteLine($"{report.Text} (progress)");
                    return true;
                case ProgessStatus.Completed:
                    Console.WriteLine($"{report.Text} (completed)");
                    return false;

                default:
                    throw new NotSupportedException(
                        $"{nameof(ProgessStatus)} with {report.ProgessStatus}");
            }
        }
    }
}