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
using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.State;

namespace KustoCopyConsole.Runner
{
    internal class MainRunner : RunnerBase, IAsyncDisposable
    {
        #region Constructors
        internal static async Task<MainRunner> CreateAsync(
            MainJobParameterization parameterization,
            string authentication,
            string logFilePath,
            CancellationToken ct)
        {
            var appendStorage = CreateAppendStorage(logFilePath);
            var rowItemGateway = await RowItemGateway.CreateAsync(appendStorage, ct);
            var dbClientFactory = await DbClientFactory.CreateAsync(
                parameterization,
                CreateCredentials(authentication),
                ct);

            return new MainRunner(parameterization, rowItemGateway, dbClientFactory);
        }

        private MainRunner(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
            : base(parameterization, rowItemGateway, dbClientFactory)
        {
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

        private static IAppendStorage CreateAppendStorage(string logFilePath)
        {
            return new LocalAppendStorage(GetLocalLogFilePath(logFilePath));
        }

        private static string GetLocalLogFilePath(string logFilePath)
        {
            const string DEFAULT_FILE_NAME = "kusto-copy.log";

            if (string.IsNullOrWhiteSpace(logFilePath))
            {
                return DEFAULT_FILE_NAME;
            }
            else if (Directory.Exists(logFilePath))
            {
                return Path.Combine(logFilePath, DEFAULT_FILE_NAME);
            }
            else
            {
                return logFilePath;
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)RowItemGateway).DisposeAsync();
            ((IDisposable)DbClientFactory).Dispose();
        }


        public async Task RunAsync(CancellationToken ct)
        {
            var tableIterationRunner =
                new TableIterationRunner(Parameterization, RowItemGateway, DbClientFactory);
            var sourceTableIdentities = Parameterization.Activities
                .Select(a => a.Source.GetTableIdentity())
                .ToImmutableArray();

            while (true)
            {
                var runTasks = sourceTableIdentities
                    .Select(i => tableIterationRunner.RunAsync(i, ct))
                    .ToImmutableArray();

                await Task.WhenAll(runTasks);
            }
        }
    }
}