using Kusto.Data.Exceptions;
using KustoCopyConsole.Concurrency;
using Polly;
using System.Net.Sockets;

namespace KustoCopyConsole.Kusto
{
    internal abstract class KustoClientBase
    {
        private static AsyncPolicy _kustoRetryPolicy = Policy
            .Handle<KustoException>(ex => ShouldFailException(ex))
            .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(Math.Max(30, Math.Pow(2, retryAttempt))));

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

        private static bool ShouldFailException(KustoException ex)
        {
            if (!ex.IsPermanent)
            {
                return false;
            }
            else if (ex.InnerException is KustoClientRequestCanceledByUserException)
            {
                return false;
            }
            else if (ex is KustoRequestThrottledException te)
            {
                return false;
            }
            //  Network transient errors
            else if (ex.InnerException is SocketException || ex.InnerException is IOException)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }
}