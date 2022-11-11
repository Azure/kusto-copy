using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Parameters
{
    public class MainParameterization
    {
        public string? LakeFolderConnectionString { get; set; }

        public SourceParameterization? Source { get; set; }

        public DestinationParameterization? Destination { get; set; }

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(LakeFolderConnectionString))
            {
                throw new CopyException(
                    $"{nameof(LakeFolderConnectionString)} isn't specified or bad format:  '{LakeFolderConnectionString}'");
            }
            if (Source == null && Destination == null)
            {
                throw new CopyException(
                    $"Neither {nameof(Source)} nor {nameof(Destination)} is specified");
            }
            if (Source != null)
            {
                Source.Validate();
            }
            if (Destination != null)
            {
                Destination.Validate();
            }
        }

        public static MainParameterization Create(CommandLineOptions options)
        {
            var parameterization = new MainParameterization
            {
                LakeFolderConnectionString = options.LakeFolderConnectionString,
                Source = new SourceParameterization
                {
                    ClusterQueryConnectionString = options.SourceConnectionString,
                    ConcurrentQueryCount = options.ConcurrentQueryCount,
                    ConcurrentExportCommandCount = options.ConcurrentExportCommandCount,
                    Databases = new[] { options.Db! }
                    .Select(db => new SourceDatabaseParameterization
                    {
                        Name = db,
                        TablesToInclude = options.TablesToInclude.ToImmutableArray(),
                        TablesToExclude = options.TablesToExclude.ToImmutableArray()
                    }).ToImmutableArray()
                },
                Destination = !string.IsNullOrWhiteSpace(options.DestinationConnectionString)
                ? new DestinationParameterization
                {
                    ClusterQueryConnectionString = options.DestinationConnectionString,
                    ConcurrentQueryCount = options.ConcurrentQueryCount,
                    ConcurrentIngestionCount = options.ConcurrentIngestionCount
                }
                : null
            };

            return parameterization;
        }
    }
}