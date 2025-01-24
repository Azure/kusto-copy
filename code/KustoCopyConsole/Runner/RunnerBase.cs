﻿using Azure.Core;
using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using KustoCopyConsole.Storage.AzureStorage;
using System.Diagnostics;

namespace KustoCopyConsole.Runner
{
    internal class RunnerBase
    {
        private static readonly TraceSource _traceSource = new(TraceConstants.TRACE_SOURCE);

        private readonly TimeSpan _wakePeriod;
        private volatile TaskCompletionSource _wakeUpSource = new TaskCompletionSource();

        public RunnerBase(
            MainJobParameterization parameterization,
            TokenCredential credential,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            IStagingBlobUriProvider stagingBlobUriProvider,
            TimeSpan wakePeriod)
        {
            Parameterization = parameterization;
            Credential = credential;
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
            StagingBlobUriProvider = stagingBlobUriProvider;
            _wakePeriod = wakePeriod;
            rowItemGateway.RowItemAppended += (sender, e) =>
            {
                if (IsWakeUpRelevant(e))
                {
                    var wakeUpSource = Interlocked.Exchange(
                        ref _wakeUpSource,
                        new TaskCompletionSource());

                    wakeUpSource.TrySetResult();
                }
            };
        }

        protected MainJobParameterization Parameterization { get; }

        protected TokenCredential Credential { get; }

        protected RowItemGateway RowItemGateway { get; }

        protected DbClientFactory DbClientFactory { get; }

        protected IStagingBlobUriProvider StagingBlobUriProvider { get; }

        protected Task WakeUpTask => _wakeUpSource.Task;

        protected bool AllActivitiesCompleted()
        {
            return !RowItemGateway.InMemoryCache.ActivityMap
                .Values
                .Where(a => a.RowItem.State == ActivityState.Active)
                .Any();
        }

        /// <summary>
        /// This method is called when a new <see cref="RowItemBase"/> is appended to the cache.
        /// If this method deems it "relevant", it will trigger <see cref="WakeUpTask"/>.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        protected virtual bool IsWakeUpRelevant(RowItemBase item)
        {
            return false;
        }

        protected Task SleepAsync(CancellationToken ct)
        {
            return Task.WhenAny(
                Task.Delay(_wakePeriod, ct),
                WakeUpTask);
        }

        protected void TraceWarning(string text)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, text);
        }
    }
}