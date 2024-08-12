using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameter
{
    public class ActivityParameterization
    {
        public TableParameterization Source { get; set; } = new();

        public IImmutableList<TableParameterization> Destinations { get; set; } =
            ImmutableArray<TableParameterization>.Empty;

        public string Query { get; set; } = string.Empty;

        public TableOption TableOption { get; set; } = new();

        internal void Validate()
        {
            Source.Validate();
            if (string.IsNullOrWhiteSpace(Source.DatabaseName))
            {
                throw new CopyException("Source database name is required", false);
            }
            if (string.IsNullOrWhiteSpace(Source.TableName))
            {
                throw new CopyException("Source table name is required", false);
            }
            foreach (var d in Destinations)
            {
                d.Validate();
            }
            TableOption.Validate();
        }
    }
}