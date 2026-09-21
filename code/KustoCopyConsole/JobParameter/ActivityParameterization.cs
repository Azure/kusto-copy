using KustoCopyConsole.Entity;

namespace KustoCopyConsole.JobParameter
{
    public class ActivityParameterization
    {
        public string ActivityName { get; set; } = string.Empty;

        public SourceTableParameterization Source { get; set; } = new();

        public DestinationTableParameterization Destination { get; set; } = new();

        public string KqlQuery { get; set; } = string.Empty;

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(ActivityName))
            {
                throw new CopyException($"{nameof(ActivityName)} is required", false);
            }
            Source.Validate();
            Destination.Validate();
        }

        public TableIdentity GetSourceTableIdentity()
        {
            return Source.GetTableIdentity();
        }

        public TableIdentity GetDestinationTableIdentity()
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