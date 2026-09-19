using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal abstract class RunnerBase
    {
        private static readonly TraceSource _traceSource = new(TraceConstants.TRACE_SOURCE);

        private readonly TimeSpan _wakePeriod;
        private readonly TaskCompletionSource _allActivityCompletedSource
            = new TaskCompletionSource();

        public RunnerBase(RunnerParameters parameters, TimeSpan wakePeriod)
        {
            RunnerParameters = parameters;
            _wakePeriod = wakePeriod;
        }

        public abstract Task RunAsync(CancellationToken ct);

        protected RunnerParameters RunnerParameters { get; }

        protected MainJobParameterization Parameterization => RunnerParameters.Parameterization;

        protected TrackDatabase Database => RunnerParameters.Database;

        protected DbClientFactory DbClientFactory => RunnerParameters.DbClientFactory;

        protected AzureBlobUriProvider StagingBlobUriProvider => RunnerParameters.StagingBlobUriProvider;

        protected bool ShouldExportRun => Parameterization.CopyFlow != CopyFlow.IngestOnly;

        protected bool ShouldIngestionRun => Parameterization.CopyFlow != CopyFlow.ExportOnly;

        protected bool ShouldRunnersContinue()
        {
            using (var tx = Database.CreateTransaction())
            {
                if (Parameterization.IterationPeriod != null)
                {
                    return true;
                }
                else if (Parameterization.CopyFlow != CopyFlow.ExportOnly)
                {
                    var areAllCompleted = Database.Activities.Query(tx)
                        .Where(pf => pf.NotEqual(a => a.State, ActivityState.Completed))
                        .Count() == 0;
                    var isActive = !(areAllCompleted && Parameterization.IterationPeriod == null);

                    if (!isActive)
                    {
                        _allActivityCompletedSource.TrySetResult();
                    }

                    return isActive;
                }
                else
                {
                    var areIterationPlanning = Database.Iterations.Query(tx)
                        .Where(pf => pf.Equal(i => i.State, IterationState.Planning))
                        .Count() != 0;

                    if (areIterationPlanning)
                    {
                        return true;
                    }
                    else
                    {
                        var plannedIterationKeys = Database.Iterations.Query(tx)
                            .Where(pf => pf.Equal(i => i.State, IterationState.Planning))
                            .Select(i => i.IterationKey);

                        foreach (var iterationKey in plannedIterationKeys)
                        {
                            var metricMap = Database.QueryAggregatedBlockMetrics(iterationKey, tx);

                            foreach (var p in metricMap)
                            {
                                var metric = p.Key;
                                var cardinality = p.Value;

                                if (metric < BlockMetric.Exported && cardinality > 0)
                                {
                                    return true;
                                }
                            }
                        }

                        return false;
                    }
                }
            }
        }

        protected async Task SleepAsync(CancellationToken ct)
        {
            await Task.WhenAny(
                _allActivityCompletedSource.Task,
                Task.Delay(_wakePeriod, ct));

            if (ct.IsCancellationRequested)
            {
                Trace.TraceInformation("");
                Trace.TraceInformation($"General failure:  {GetType().Name}");
                Trace.TraceInformation("");
            }
            ct.ThrowIfCancellationRequested();
        }

        protected void TraceWarning(string text)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, text);
        }

        #region Temp Table
        protected TempTableRecord? TryGetTempTable(IterationKey iterationKey)
        {
            var tempTable = Database.TempTables.Query()
                .Where(pf => pf.Equal(t => t.IterationKey.ActivityName, iterationKey.ActivityName))
                .Where(pf => pf.Equal(t => t.IterationKey.IterationId, iterationKey.IterationId))
                .Where(pf => pf.Equal(t => t.State, TempTableState.Created))
                .Take(1)
                .FirstOrDefault();

            return tempTable;
        }

        protected TempTableRecord GetTempTable(IterationKey iterationKey)
        {
            var tempTable = TryGetTempTable(iterationKey);

            if (tempTable == null)
            {
                throw new InvalidDataException(
                    $"TempTable for iteration {iterationKey} should exist by now");
            }

            return tempTable;
        }
        #endregion
    }
}