using System.Collections.Immutable;

namespace KustoCopyConsole.Parameters
{
    public class SourceDatabaseParameterization
    {
        public string? Name { get; set; }

        public DatabaseOverrideParameterization? DatabaseOverrides { get; set; }

        public IImmutableList<string> TablesToInclude { get; set; } = ImmutableArray<string>.Empty;

        public IImmutableList<string> TablesToExclude { get; set; } = ImmutableArray<string>.Empty;

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(Name))
            {
                throw new CopyException($"{nameof(Name)} isn't specified");
            }
        }

        public SourceDatabaseParameterization Override(
            DatabaseConfigParameterization databaseDefault)
        {
            return new SourceDatabaseParameterization
            {
                Name = Name,
                TablesToExclude = TablesToExclude,
                TablesToInclude = TablesToInclude,
                DatabaseOverrides = new DatabaseOverrideParameterization
                {
                    IsEnabled = DatabaseOverrides?.IsEnabled ?? databaseDefault.IsEnabled,
                    BackfillHorizon = DatabaseOverrides?.BackfillHorizon
                    ?? databaseDefault.BackfillHorizon,
                    Rpo = DatabaseOverrides?.Rpo ?? databaseDefault.Rpo,
                }
            };
        }
    }
}