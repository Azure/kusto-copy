﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;

namespace KustoCopyConsole.JobParameter
{
    internal class MainJobParameterization
    {
        public bool IsContinuousRun { get; set; } = false;

        public IImmutableList<ActivityParameterization> Activities { get; set; } =
            ImmutableArray<ActivityParameterization>.Empty;

        public static MainJobParameterization FromOptions(CommandLineOptions options)
        {
            if (!string.IsNullOrWhiteSpace(options.Source))
            {
                if (string.IsNullOrWhiteSpace(options.Destination))
                {
                    throw new CopyException(
                        $"Source is specified ('options.Source'):  destination is expected",
                        false);
                }

                if (!Uri.TryCreate(options.Source, UriKind.Absolute, out var source))
                {
                    throw new CopyException($"Can't parse source:  '{options.Source}'", false);
                }
                if (!Uri.TryCreate(options.Destination, UriKind.Absolute, out var destination))
                {
                    throw new CopyException(
                        $"Can't parse destination:  '{options.Destination}'",
                        false);
                }
                var sourceBuilder = new UriBuilder(source);
                var sourcePathParts = sourceBuilder.Path.Split('/');
                var destinationBuilder = new UriBuilder(destination);
                var destinationPathParts = destinationBuilder.Path.Split('/');

                if (sourcePathParts.Length != 3)
                {
                    throw new CopyException(
                        $"Source ('{options.Source}') should be of the form 'https://help.kusto.windows.net/Samples/nyc_taxi'",
                        false);
                }
                if (destinationPathParts.Length != 2)
                {
                    throw new CopyException(
                        $"Destination ('{options.Destination}') should be of the form 'https://mycluster.eastus.kusto.windows.net/mydb'",
                        false);
                }

                var sourceDb = sourcePathParts[1];
                var sourceTable = sourcePathParts[2];
                var destinationDb = destinationPathParts[1];

                sourceBuilder.Path = string.Empty;
                destinationBuilder.Path = string.Empty;

                return new MainJobParameterization
                {
                    IsContinuousRun = options.IsContinuousRun,
                    Activities = ImmutableList.Create(
                        new ActivityParameterization
                        {
                            Source = new TableParameterization
                            {
                                ClusterUri = sourceBuilder.ToString(),
                                DatabaseName = sourceDb,
                                TableName = sourceTable
                            },
                            Destination = new TableParameterization
                            {
                                ClusterUri = destinationBuilder.ToString(),
                                DatabaseName = destinationDb
                            },
                            Query = options.Query,
                            TableOption = new TableOption()
                        })
                };
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal void Validate()
        {
            foreach (var a in Activities)
            {
                a.Validate();
            }
        }

        internal string ToYaml()
        {
            var serializer = new SerializerBuilder().Build();
            var yaml = serializer.Serialize(this);

            return yaml;
        }
    }
}