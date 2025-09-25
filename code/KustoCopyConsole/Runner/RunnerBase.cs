using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.Keys;
using KustoCopyConsole.Entity.State;
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

        protected RunnerParameters RunnerParameters { get; }

        protected bool AreActivitiesCompleted(params IEnumerable<string> activityNames)
        {
            var isCompleted = RunnerParameters.Database.Activities.Query()
                .Where(pf => pf.In(a => a.ActivityName, activityNames))
                .Where(pf => pf.Equal(a => a.State, ActivityState.Active))
                .Count() == 0;

            return isCompleted;
        }

        protected bool AllActivitiesCompleted()
        {
            var allCompleted = !RunnerParameters.Database.Activities.Query()
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

        #region Temp Table
        protected TempTableRecord? TryGetTempTable(IterationKey iterationKey)
        {
            var tempTable = RunnerParameters.Database.TempTables.Query()
                .Where(pf => pf.Equal(t => t.IterationKey.ActivityName, iterationKey.ActivityName))
                .Where(pf => pf.Equal(t => t.IterationKey.IterationId, iterationKey.IterationId))
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