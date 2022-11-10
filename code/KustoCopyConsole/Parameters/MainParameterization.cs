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

        internal static MainParameterization Create(CommandLineOptions options)
        {
            var parameterization = new MainParameterization
            {
                LakeFolderUri = options.LakeFolderUri,
                Source = new SourceParameterization
                {
                    ClusterQueryConnectionString = options.SourceConnectionString,
                    ConcurrentQueryCount = options.ConcurrentQueryCount,
                    ConcurrentExportCommandCount = options.ConcurrentExportCommandCount,
                    Databases = options.Dbs.Select(db => new SourceDatabaseParameterization
                    {
                        Name = db
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