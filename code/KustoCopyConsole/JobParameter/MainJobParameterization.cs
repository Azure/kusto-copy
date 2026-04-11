using Azure.Core;
using Azure.Identity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace KustoCopyConsole.JobParameter
{
    internal class MainJobParameterization
    {
        public List<ActivityParameterization> Activities { get; set; } = new();

        public TableOption TableOption { get; set; } = new TableOption();

        public bool IsContinuousRun { get; set; } = false;

        public int? ExportCount { get; set; } = null;

        public bool KeepTags { get; private set; }

        public List<string> StagingStorageDirectories { get; set; } = new();

        public string ManagedIdentityClientId { get; set; } = string.Empty;

        #region Constructors
        public static MainJobParameterization FromOptions(CommandLineOptions options)
        {
            var parameterization = string.IsNullOrWhiteSpace(options.Yaml)
                ? new MainJobParameterization()
                : LoadYaml(options.Yaml);

            if (options.StagingStorageDirectories.Any())
            {
                parameterization.StagingStorageDirectories =
                    options.StagingStorageDirectories.ToList();
            }
            if (!string.IsNullOrWhiteSpace(options.Source))
            {
                if (string.IsNullOrWhiteSpace(options.Destination))
                {
                    throw new CopyException(
                        "Destination is expected when source is provided",
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

                var sourceBuilder = new UriBuilder(options.Source);
                var destinationBuilder = new UriBuilder(options.Destination);
                var sourcePathParts = sourceBuilder.Path.Split('/');
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

                var activity = new ActivityParameterization
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
                    KqlQuery = options.Query.Trim()
                };

                parameterization.Activities = new([activity]);
            }
            else if (!string.IsNullOrWhiteSpace(options.Destination))
            {
                throw new CopyException(
                    "Source is expected when destination is provided",
                    false);
            }
            if (!string.IsNullOrWhiteSpace(options.ManagedIdentityClientId))
            {
                parameterization.ManagedIdentityClientId = options.ManagedIdentityClientId;
            }
            if (options.IsContinuousRun != null)
            {
                parameterization.IsContinuousRun = options.IsContinuousRun.Value;
            }
            if (options.ExportCount != null)
            {
                parameterization.ExportCount = options.ExportCount;
            }
            if (options.KeepTags != null)
            {
                parameterization.KeepTags = options.KeepTags.Value;
            }

            parameterization.Validate();

            return parameterization;
        }
        #endregion

        public ActivityParameterization GetActivity(string activityName)
        {
            foreach (var activityParameterization in Activities)
            {
                if (activityParameterization.ActivityName == activityName)
                {
                    return activityParameterization;
                }
            }

            throw new ArgumentException(
                $"Activity '{activityName}' is not found",
                nameof(activityName));
        }

        internal void Validate()
        {
            if (StagingStorageDirectories.Count == 0)
            {
                throw new CopyException("Staging directories are expected", false);
            }
            foreach (var a in Activities)
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
            var options = new DefaultAzureCredentialOptions
            {
                CredentialProcessTimeout = TimeSpan.FromSeconds(15)
            };

            if (string.IsNullOrWhiteSpace(ManagedIdentityClientId))
            {
                options.ManagedIdentityClientId = ManagedIdentityClientId;
            }

            return new DefaultAzureCredential(options);
        }

        internal string ToYaml()
        {
            var serializer = new SerializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            var yaml = serializer.Serialize(this);

            return yaml;
        }

        private static MainJobParameterization LoadYaml(string yamlPath)
        {
            var yamlContent = File.ReadAllText(yamlPath);
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();
            var parameterization = deserializer.Deserialize<MainJobParameterization>(yamlContent);

            return parameterization;
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