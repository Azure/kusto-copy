using KustoCopyConsole.Entity;
using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameter
{
    public class ActivityParameterization
    {
        public TableParameterization Source { get; set; } = new();

        public TableParameterization Destination { get; set; } = new();

        public string Query { get; set; } = string.Empty;

        public TableOption TableOption { get; set; } = new();

        public void Validate()
        {
            Source.Validate();
            if (string.IsNullOrWhiteSpace(Source.TableName))
            {
                throw new CopyException($"{nameof(Source.TableName)} is required", false);
            }
            Destination.Validate();
            TableOption.Validate();
        }

        public TableIdentity GetEffectiveDestinationTableIdentity()
        {
            var destinationTableIdentity = Destination.GetTableIdentity();

            return !string.IsNullOrWhiteSpace(Destination.TableName)
                ? destinationTableIdentity
                : new TableIdentity(
                    destinationTableIdentity.ClusterUri,
                    destinationTableIdentity.DatabaseName,
                    Source.GetTableIdentity().TableName);
        }
    }
}