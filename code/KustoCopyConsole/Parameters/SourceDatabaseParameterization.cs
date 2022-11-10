using System.Collections.Immutable;

namespace KustoCopyConsole.Parameters
{
    public class SourceDatabaseParameterization
    {
        public string? Name { get; set; }

        public DatabaseOverrideParameterization? DatabaseOverrides { get; set; }
        
        public DestinationDatabaseParameterization? Destination { get; set; }
    }
}