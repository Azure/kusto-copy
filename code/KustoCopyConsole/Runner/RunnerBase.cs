using Azure.Core;
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
        private readonly Queue<Task> _wakeUpTaskQueue = new();
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

                    //  Wake up on a different thread not to block the current one
                    EnqueueWakeUpTask(Task.Run(() => wakeUpSource.TrySetResult()));
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

        protected async Task SleepAsync(CancellationToken ct)
        {
            await CleanWakeUpTaskQueueAsync(ct);
            await Task.WhenAny(
                Task.Delay(_wakePeriod, ct),
                WakeUpTask);
        }

        protected void TraceWarning(string text)
        {
            _traceSource.TraceEvent(TraceEventType.Warning, 0, text);
        }

        private async Task CleanWakeUpTaskQueueAsync(CancellationToken ct)
        {   //  Many threads could de-queue at the same time
            while (_wakeUpTaskQueue.TryDequeue(out var task))
            {
                if (task.IsCompleted)
                {
                    await task;
                }
                else
                {
                    EnqueueWakeUpTask(task);

                    return;
                }
            }
        }

        private void EnqueueWakeUpTask(Task task)
        {
            if (task == null)
            {
                throw new ArgumentNullException(nameof(task));
            }
            _wakeUpTaskQueue.Enqueue(task);
        }
    }
}