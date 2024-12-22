using Azure.Core;
using Azure.Identity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage.LocalDisk;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : IAsyncDisposable
    {
        private readonly RowItemGateway _rowItemGateway;
        private readonly DbClientFactory _dbClientFactory;

        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var parameterization = CreateParameterization(options);

            var appendStorage = CreateAppendStorage(options);
            var rowItemGateway = await RowItemGateway.CreateAsync(appendStorage, ct);
            var dbClientFactory = await DbClientFactory.CreateAsync(
                parameterization,
                CreateCredentials(options.Authentication),
                ct);

            return new MainRunner(parameterization, dbClientFactory, rowItemGateway);
        }

        private MainRunner(
            MainJobParameterization parameterization,
            DbClientFactory dbClientFactory,
            RowItemGateway rowItemGateway)
        {
            Parameterization = parameterization;
            _dbClientFactory = dbClientFactory;
            _rowItemGateway = rowItemGateway;
        }

        private static TokenCredential CreateCredentials(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication))
            {
                //return new DefaultAzureCredential();
                return new AzureCliCredential();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static IAppendStorage CreateAppendStorage(CommandLineOptions options)
        {
            return new LocalAppendStorage(GetLocalLogFilePath(options));
        }

        private static string GetLocalLogFilePath(CommandLineOptions options)
        {
            const string DEFAULT_FILE_NAME = "kusto-copy.log";

            if (string.IsNullOrWhiteSpace(options.LogFilePath))
            {
                return DEFAULT_FILE_NAME;
            }
            else if (Directory.Exists(options.LogFilePath))
            {
                return Path.Combine(options.LogFilePath, DEFAULT_FILE_NAME);
            }
            else
            {
                return options.LogFilePath;
            }
        }

        private static MainJobParameterization CreateParameterization(CommandLineOptions options)
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
                var destinationDb = sourcePathParts[1];

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
                            Destinations = ImmutableList.Create(new TableParameterization
                            {
                                ClusterUri = destinationBuilder.ToString(),
                                DatabaseName = destinationDb
                            }),
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
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_rowItemGateway).DisposeAsync();
            ((IDisposable)_dbClientFactory).Dispose();
        }

        public MainJobParameterization Parameterization { get; }

        public async Task RunAsync(CancellationToken token)
        {
            await Task.CompletedTask;

            return;
        }
    }
}