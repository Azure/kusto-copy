using Azure.Core;
using KustoCopyConsole.Db;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class RunnerBase
    {
        private static readonly TraceSource _traceSource = new(TraceConstants.TRACE_SOURCE);

        private readonly TimeSpan _wakePeriod;
        private readonly TaskCompletionSource _allActivityCompletedSource
            = new TaskCompletionSource();

        public RunnerBase(
            MainJobParameterization parameterization,
            TokenCredential credential,
            TrackDatabase database,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider,
            TimeSpan wakePeriod)
        {
            Parameterization = parameterization;
            Credential = credential;
            Database = database;
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
            StagingBlobUriProvider = stagingBlobUriProvider;
            _wakePeriod = wakePeriod;
        }

        protected MainJobParameterization Parameterization { get; }

        protected TokenCredential Credential { get; }

        protected TrackDatabase Database { get; }

        protected RowItemGateway RowItemGateway { get; }

        protected DbClientFactory DbClientFactory { get; }

        protected IStagingBlobUriProvider StagingBlobUriProvider { get; }

        protected bool AllActivitiesCompleted()
        {
            var allCompleted = !Database.Activities.Query()
                .Where(pf => pf.Equal(a => a.State, ActivityState.Active))
                .Any();

            if (allCompleted)
            {
                _allActivityCompletedSource.TrySetResult();
            }

            return allCompleted;
        }

        protected async Task SleepAsync(CancellationToken ct)
        {
            await Task.WhenAny(_allActivityCompletedSource.Task, Task.Delay(_wakePeriod, ct));
        }

        protected void TraceWarning(string text)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, text);
        }
    }
}