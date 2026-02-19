using Kusto.Data.Exceptions;
using KustoCopyConsole.Concurrency;
using Polly;
using System.Diagnostics;
using System.Net.Sockets;

namespace KustoCopyConsole.Kusto
{
    internal abstract class KustoClientBase
    {
        private static AsyncPolicy _kustoRetryPolicy = Policy
            .Handle<KustoException>(ex => ShouldRetryException(ex))
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

        private static bool ShouldRetryException(KustoException ex)
        {
            if (!ex.IsPermanent
                || ex.InnerException is KustoClientRequestCanceledByUserException
                || ex is KustoRequestThrottledException
                //  Network transient errors
                || ex.InnerException is SocketException
                || ex.InnerException is IOException)
            {
                Trace.TraceWarning($"Transient error:  {ex.GetType().Name} '{ex.Message}'");
                if (ex.InnerException != null)
                {
                    Trace.TraceWarning($"   Inner:  {ex.InnerException.GetType().Name}" +
                        $" '{ex.InnerException.Message}'");
                }
                Trace.TraceWarning($"Stack trace:  {ex.StackTrace}");

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}