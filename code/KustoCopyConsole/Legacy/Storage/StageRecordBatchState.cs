using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class StageRecordBatchState
    {
        public IImmutableList<string> ExtentIds { get; set; } = ImmutableList<string>.Empty;

        public string TagValue { get; set; } = string.Empty;
    }
}