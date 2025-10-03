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
        }

        public TableIdentity GetSourceTableIdentity()
        {
            var sourceTableIdentity = Source.GetTableIdentity();

            return !string.IsNullOrWhiteSpace(Destination.TableName)
                ? sourceTableIdentity
                : new TableIdentity(
                    NormalizedUri.NormalizeUri(sourceTableIdentity.ClusterUri.ToString()),
                    sourceTableIdentity.DatabaseName,
                    Source.GetTableIdentity().TableName);
        }

        public TableIdentity GetDestinationTableIdentity()
        {
            var destinationTableIdentity = Destination.GetTableIdentity();

            return !string.IsNullOrWhiteSpace(Destination.TableName)
                ? destinationTableIdentity
                : new TableIdentity(
                    NormalizedUri.NormalizeUri(destinationTableIdentity.ClusterUri.ToString()),
                    destinationTableIdentity.DatabaseName,
                    Source.GetTableIdentity().TableName);
        }
    }
}