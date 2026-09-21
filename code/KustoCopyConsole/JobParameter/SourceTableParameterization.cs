namespace KustoCopyConsole.JobParameter
{
    public class SourceTableParameterization : TableParameterization
    {
        public string? EntityGroup { get; set; }

        public override void Validate()
        {
            base.Validate();
            if (string.IsNullOrWhiteSpace(TableName))
            {
                throw new CopyException($"{nameof(TableName)} is required", false);
            }
        }
    }
}