using Azure.Core;
using Azure.Identity;
using System;
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
        public IImmutableDictionary<string, ActivityParameterization> Activities { get; set; } =
            ImmutableDictionary<string, ActivityParameterization>.Empty;

        public bool IsContinuousRun { get; set; } = false;

        public IImmutableList<string> StagingStorageDirectories { get; set; } =
            ImmutableArray<string>.Empty;

        public string Authentication { get; set; } = string.Empty;

        #region Constructors
        public static MainJobParameterization FromOptions(CommandLineOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.Source))
            {
                throw new CopyException($"Source is expected", false);
            }
            if (string.IsNullOrWhiteSpace(options.Destination))
            {
                throw new CopyException($"Destination is expected", false);
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
            if (destinationPathParts.Length < 2 || destinationPathParts.Length > 3)
            {
                throw new CopyException(
                    $"Destination ('{options.Destination}') should be of the form " +
                    $"'https://mycluster.eastus.kusto.windows.net/mydb' or " +
                    $"'https://mycluster.eastus.kusto.windows.net/mydb/mytable'",
                    false);
            }

            var sourceDb = sourcePathParts[1];
            var sourceTable = sourcePathParts[2];
            var destinationDb = destinationPathParts[1];
            var destinationTable = destinationPathParts.Length == 3
                ? destinationPathParts[2]
                : string.Empty;

            sourceBuilder.Path = string.Empty;
            destinationBuilder.Path = string.Empty;

            var parameterization = new MainJobParameterization
            {
                IsContinuousRun = options.IsContinuousRun,
                Activities = ImmutableList.Create(
                    new ActivityParameterization
                    {
                        ActivityName = "default",
                        Source = new TableParameterization
                        {
                            ClusterUri = sourceBuilder.ToString(),
                            DatabaseName = sourceDb,
                            TableName = sourceTable
                        },
                        Destination = new TableParameterization
                        {
                            ClusterUri = destinationBuilder.ToString(),
                            DatabaseName = destinationDb,
                            TableName = destinationTable
                        },
                        KqlQuery = options.Query.Trim(),
                        TableOption = new TableOption()
                    })
                .ToImmutableDictionary(a => a.ActivityName, a => a),
                Authentication = options.Authentication,
                StagingStorageDirectories = options.StagingStorageDirectories.ToImmutableArray()
            };

            parameterization.Validate();

            return parameterization;
        }
        #endregion

        internal void Validate()
        {
            foreach (var a in Activities.Values)
            {
                a.Validate();
            }
            foreach (var uri in StagingStorageDirectories)
            {
                ValidateStagingUri(uri);
            }
        }

        internal TokenCredential CreateCredentials()
        {
            if (string.IsNullOrWhiteSpace(Authentication))
            {
                return new DefaultAzureCredential(true);
                //return new AzureCliCredential();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal string ToYaml()
        {
            var serializer = new SerializerBuilder().Build();
            var yaml = serializer.Serialize(this);

            return yaml;
        }

        private void ValidateStagingUri(string uriText)
        {
            if (!Uri.TryCreate(uriText, UriKind.Absolute, out var uri))
            {
                throw new CopyException($"Not a valid staging URI:  '{uri}'", false);
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(uri.Query))
                {
                    throw new CopyException(
                        $"{nameof(StagingStorageDirectories)} can't contain query string:  '{uri}'",
                        false);
                }
                if (uri.Segments.Length < 2)
                {
                    throw new CopyException(
                        $"{nameof(StagingStorageDirectories)} should point at " +
                        $"least to a container:  '{uri}'",
                        false);
                }
            }
        }
    }
}