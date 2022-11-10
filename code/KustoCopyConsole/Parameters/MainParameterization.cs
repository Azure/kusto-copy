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
        public Uri? LakeFolderUri { get; set; }

        public SourceParameterization? Source { get; set; }

        public DestinationParameterization? Destination { get; set; }

        public void Validate()
        {
            if (LakeFolderUri == null || !LakeFolderUri.IsAbsoluteUri)
            {
                throw new CopyException(
                    $"{nameof(LakeFolderUri)} isn't specified or bad format:  '{LakeFolderUri}'");
            }
            if (Source == null)
            {
                throw new CopyException($"{nameof(Source)} isn't specified");
            }
            Source.Validate();
        }

        public static MainParameterization Create(CommandLineOptions options)
        {
            var parameterization = new MainParameterization
            {
                LakeFolderUri = options.LakeFolderUri,
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
                Destination = new DestinationParameterization
                {
                    ClusterQueryConnectionString = options.DestinationConnectionString,
                    ConcurrentQueryCount = options.ConcurrentQueryCount,
                    ConcurrentIngestionCount = options.ConcurrentIngestionCount
                }
            };

            return parameterization;
        }
    }
}