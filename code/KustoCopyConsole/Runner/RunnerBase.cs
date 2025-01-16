﻿using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;

namespace KustoCopyConsole.Runner
{
    internal class RunnerBase
    {
        private volatile TaskCompletionSource _wakeUpSource = new TaskCompletionSource();

        public RunnerBase(
            MainJobParameterization parameterization,
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory)
        {
            Parameterization = parameterization;
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
            rowItemGateway.InMemoryCache.RowItemAppended += (sender, e) =>
            {
                if(IsWakeUpRelevant(e))
                {
                    var wakeUpSource = _wakeUpSource;

                    wakeUpSource.TrySetResult();
                    Interlocked.CompareExchange(
                        ref _wakeUpSource,
                        new TaskCompletionSource(),
                        wakeUpSource);
                }
            };
        }

        protected MainJobParameterization Parameterization { get; }

        protected RowItemGateway RowItemGateway { get; }

        protected DbClientFactory DbClientFactory { get; }

        protected Task WakeUpTask => _wakeUpSource.Task;

        protected bool AllActivitiesCompleted()
        {
            return !RowItemGateway.InMemoryCache.ActivityMap
                .Values
                .Where(a => a.RowItem.State== ActivityState.Active)
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
    }
}