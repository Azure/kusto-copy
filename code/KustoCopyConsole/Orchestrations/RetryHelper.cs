using Kusto.Data.Exceptions;
using Polly.Retry;
using Polly;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestrations
{
    public class RetryHelper
    {
        public static AsyncRetryPolicy RetryNonPermanentKustoErrorPolicy { get; } = Policy
            .Handle<KustoRequestThrottledException>()
            .WaitAndRetryForeverAsync(
            attempt => TimeSpan.FromSeconds(0.5),
            TraceException);

        private static void TraceException(Exception ex, TimeSpan ts)
        {
            Trace.TraceWarning($"Transient error:  {ex.Message}");
        }
    }
}