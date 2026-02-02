using Kusto.Data.Exceptions;
using KustoCopyConsole.Concurrency;
using Polly;

namespace KustoCopyConsole.Kusto
{
    internal abstract class KustoClientBase
    {
        private static AsyncPolicy _kustoRetryPolicy = Policy
            .Handle<KustoException>(ex => !ex.IsPermanent)
            .RetryAsync(5);

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
    }
}