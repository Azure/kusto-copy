
using Azure.Core;
using Azure.Identity;
using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using KustoCopyConsole.Storage.LocalDisk;
using System.Collections.Immutable;

namespace KustoCopyConsole.Orchestration
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly RowItemGateway _rowItemGateway;
        private readonly DbClientFactory _dbClientFactory;

        #region Constructors
        internal static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var parameterization = CreateParameterization(options);

            var appendStorage = CreateAppendStorage();
            var rowItemGateway = await RowItemGateway.CreateAsync(appendStorage, ct);
            var dbClientFactory = await DbClientFactory.CreateAsync(
                parameterization,
                CreateCredentials(options.Authentication),
                ct);

            return new MainOrchestration(parameterization, dbClientFactory, rowItemGateway);
        }

        private MainOrchestration(
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
                return new DefaultAzureCredential();
            }
            else
            {
                throw new NotImplementedException();
            }
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
                            TableOption = new TableOption
                            {
                                IsContinuousRun = options.IsContinuousRun
                            }
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

        internal async Task ProcessAsync(CancellationToken ct)
        {
            var sourceTableIteration = new SourceTableIterationOrchestration(
                _rowItemGateway,
                _dbClientFactory,
                Parameterization);
            var sourceTablePlanning = new SourceTablePlanningOrchestration(
                _rowItemGateway,
                _dbClientFactory,
                Parameterization);
            var subOrchestrationTasks = new SubOrchestrationBase[]
            {
                sourceTableIteration,
                sourceTablePlanning
            }
            .Select(d => d.ProcessAsync(ct))
            .ToImmutableArray();

            //  Let every database orchestration complete
            await Task.WhenAll(subOrchestrationTasks);
        }
    }
}