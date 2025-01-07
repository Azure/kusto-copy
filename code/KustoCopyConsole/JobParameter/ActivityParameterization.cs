using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameter
{
    public class ActivityParameterization
    {
        public TableParameterization Source { get; set; } = new();

        public TableParameterization Destination { get; set; } = new();

        public string Query { get; set; } = string.Empty;

        public TableOption TableOption { get; set; } = new();

        internal void Validate()
        {
            Source.Validate();
            if (string.IsNullOrWhiteSpace(Source.TableName))
            {
                throw new CopyException($"{Source.TableName} is required", false);
            }
            Destination.Validate();
            TableOption.Validate();
        }
    }
}