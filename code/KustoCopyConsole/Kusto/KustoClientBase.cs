using KustoCopyConsole.Concurrency;
using Polly;
using System.Diagnostics;

namespace KustoCopyConsole.Kusto
{
    internal abstract class KustoClientBase
    {
        private static AsyncPolicy _kustoRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(1, TimeSpanToRetry, OnRetry);

        private readonly PriorityExecutionQueue<KustoPriority> _queue;

        public KustoClientBase(PriorityExecutionQueue<KustoPriority> queue)
        {
            _queue = queue;
        }

        protected async Task<T> RequestRunAsync<T>(
            KustoPriority priority,
            Func<Task<T>> actionAsync,
            CancellationToken ct)
        {
            // Retry happens within a single queue slot
            return await _queue.RequestRunAsync(
                priority,
                async () => await _kustoRetryPolicy.ExecuteAsync(_ => actionAsync(), ct));
        }

        private static TimeSpan TimeSpanToRetry(int retryAttempt)
        {
            var delay = TimeSpan.FromSeconds(Math.Min(120, Math.Pow(2, retryAttempt)));

            return delay;
        }

        private static void OnRetry(
            Exception ex,
            TimeSpan delay,
            int retryCount,
            Context context)
        {
            Trace.TraceWarning(
                $"Transient error (retryCount = {retryCount}, CorrelationId={context.CorrelationId}):" +
                $"  {ex.GetType().Name} '{ex.Message}'");
            if (ex.InnerException != null)
            {
                Trace.TraceWarning($"   Inner:  {ex.InnerException.GetType().Name}" +
                    $" '{ex.InnerException.Message}'");
            }
            Trace.TraceWarning($"Stack trace:  {ex.StackTrace}");
        }
    }
}