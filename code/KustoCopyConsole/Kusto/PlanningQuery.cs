using Kusto.Data.Exceptions;

namespace KustoCopyConsole.Kusto
{
    internal static class PlanningQuery
    {
        public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(150);

        public static bool IsResourceFailure(Exception exception)
        {
            return exception is TimeoutException
                or KustoServiceTimeoutException
                or KustoClientTimeoutException
                or KustoServicePartialQueryFailureLowMemoryConditionException
                or KustoServicePartialQueryFailureLimitsExceededException
                || exception is KustoServicePartialQueryFailureException kustoFailure
                    && (kustoFailure.Message.Contains(
                        "E_RUNAWAY_QUERY",
                        StringComparison.OrdinalIgnoreCase)
                        || kustoFailure.Message.Contains(
                            "E_LOW_MEMORY_CONDITION",
                            StringComparison.OrdinalIgnoreCase));
        }
    }
}