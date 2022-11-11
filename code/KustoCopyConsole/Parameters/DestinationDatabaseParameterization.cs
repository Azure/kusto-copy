namespace KustoCopyConsole.Parameters
{
    public class DestinationDatabaseParameterization
    {
        public string? Name { get; set; }

        public string? SourceName { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CopyException($"{nameof(Name)} isn't specified");
            }
        }
    }
}