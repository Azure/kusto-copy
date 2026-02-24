using KustoCopyConsole.Concurrency;
using Polly;
using System.Diagnostics;

namespace KustoCopyConsole.Kusto
{
    internal abstract class KustoClientBase
    {
        private static AsyncPolicy _kustoRetryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(20, TimeSpanToRetry, OnRetry);

        private readonly PriorityExecutionQueue<KustoPriority> _queue;

        public KustoClientBase(PriorityExecutionQueue<KustoPriority> queue)
        {
            _queue = queue;
        }

        protected async Task<T> RequestRunAsync<T>(
            KustoPriority priority,
            Func<Task<T>> actionAsync)
        {   //  Retry is done on a separate request:  if there is pressure on usage, we cycle the
            //  requests
            return await _kustoRetryPolicy.ExecuteAsync(
                async () => await _queue.RequestRunAsync(priority, actionAsync));
        }

        private static TimeSpan TimeSpanToRetry(int retryAttempt)
        {
            return TimeSpan.FromSeconds(Math.Min(120, Math.Pow(2, retryAttempt)));
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