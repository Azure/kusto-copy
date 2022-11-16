using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestrations
{
    public record CursorWindow(string? StartCursor, string? EndCursor)
    {
        public string ToCursorKustoPredicate()
        {
            var predicate = StartCursor == null && EndCursor == null
                ? string.Empty
                : StartCursor == null
                ? $"| where cursor_before_or_at('{EndCursor}')"
                : @$"
| where cursor_before_or_at('{EndCursor}')
| where cursor_after('{StartCursor}')";

            return predicate;
        }
    }
}