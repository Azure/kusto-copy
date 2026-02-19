using System.Diagnostics;

namespace KustoCopyConsole
{
    internal static class ErrorHelper
    {
        public static void DisplayException(Exception ex)
        {
            DisplayExceptionInternal(ex);
        }

        private static void DisplayExceptionInternal(Exception ex, string tab = "")
        {
            Trace.TraceError(
                $"{tab}Exception encountered:  {ex.GetType().Name} ; {ex.Message}");
            Trace.TraceError($"{tab}Stack trace:  {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                DisplayExceptionInternal(ex.InnerException, tab + "  ");
            }
        }
    }
}