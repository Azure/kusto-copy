
using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameters;
using KustoCopyConsole.Storage;
using KustoCopyConsole.Storage.LocalDisk;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestration
{
    internal class CopyOrchestration : IAsyncDisposable
    {
        private readonly RowItemGateway _rowItemGateway;

        #region Constructors
        internal static async Task<CopyOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var parameterization = CreateParameterization(options);
            var appendStorage = CreateAppendStorage();
            var rowItemGateway = new RowItemGateway(appendStorage, CompactItems);

            await Task.CompletedTask;

            return new CopyOrchestration(parameterization, rowItemGateway);
        }

        private CopyOrchestration(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway)
        {
            Parameterization = parameterization;
            _rowItemGateway = rowItemGateway;
        }

        private static IAppendStorage CreateAppendStorage()
        {
            return new LocalAppendStorage("kusto-copy.log");
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
                    SourceClusters = ImmutableList.Create(
                        new SourceClusterParameterization
                        {
                            IsContinuousRun = options.IsContinuousRun,
                            SourceClusterUri = sourceBuilder.ToString(),
                            Databases = ImmutableList.Create(new SourceDatabaseParameterization
                            {
                                DatabaseName = sourceDb,
                                Tables = ImmutableList.Create(new SourceTableParameterization
                                {
                                    TableName = sourceTable
                                }),
                                Destinations = ImmutableList.Create(new DestinationParameterization
                                {
                                    DestinationClusterUri = destinationBuilder.ToString(),
                                    DatabaseName = destinationDb
                                })
                            })
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
        }

        public MainJobParameterization Parameterization { get; }

        internal async Task ProcessAsync(CancellationToken ct)
        {
            var items = await _rowItemGateway.MigrateToLatestVersionAsync(ct);

            //  Allow GC
            items = null;

            throw new NotImplementedException();
        }

        private static IEnumerable<RowItem> CompactItems(IEnumerable<RowItem> items)
        {
            return items;
        }
    }
}