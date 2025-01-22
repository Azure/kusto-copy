using KustoCopyConsole.Entity;
using System.Collections.Immutable;

namespace KustoCopyConsole.JobParameter
{
    public class ActivityParameterization
    {
        public string ActivityName { get; set; } = string.Empty;

        public TableParameterization Source { get; set; } = new();

        public TableParameterization Destination { get; set; } = new();

        public string? KqlQuery { get; set; } = string.Empty;

        public TableOption TableOption { get; set; } = new();

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ActivityName))
            {
                throw new CopyException($"{nameof(ActivityName)} is required", false);
            }
            Source.Validate();
            if (string.IsNullOrWhiteSpace(Source.TableName))
            {
                throw new CopyException($"{nameof(Source.TableName)} is required", false);
            }
            Destination.Validate();
            TableOption.Validate();
            if (string.IsNullOrWhiteSpace(KqlQuery))
            {
                throw new CopyException($"{nameof(KqlQuery)} is required", false);
            }
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