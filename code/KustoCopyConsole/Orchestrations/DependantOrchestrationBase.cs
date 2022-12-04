using KustoCopyConsole.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestrations
{
    public abstract class DependantOrchestrationBase
    {
        private readonly StatusItemState _fromState;
        private readonly StatusItemState _toState;
        private readonly bool _isContinuousRun;
        private readonly Task _previousStageTask;
        private TaskCompletionSource _awaitingActivitiesSource = new TaskCompletionSource();
        private readonly ConcurrentQueue<Task> _unobservedTasksQueue =
            new ConcurrentQueue<Task>();

        protected DependantOrchestrationBase(
            StatusItemState fromState,
            StatusItemState toState,
            bool isContinuousRun,
            Task previousStageTask,
            DatabaseStatus dbStatus)
        {
            _fromState = fromState;
            _toState = toState;
            _isContinuousRun = isContinuousRun;
            _previousStageTask = previousStageTask;
            DbStatus = dbStatus;
            dbStatus.StatusChanged += (sender, e) =>
            {
                _awaitingActivitiesSource.TrySetResult();
            };
        }

        protected DatabaseStatus DbStatus { get; }

        protected abstract void QueueActivities(CancellationToken ct);

        protected async Task RunAsync(CancellationToken ct)
        {
            while (_isContinuousRun
                || !_previousStageTask.IsCompleted
                || HasWorkableIterations())
            {
                await ObserveTasksAsync();

                QueueActivities(ct);

                await RollupStatesAsync(ct);
                //  Wait for activity to continue
                await _awaitingActivitiesSource.Task;
                //  Reset task source
                _awaitingActivitiesSource = new TaskCompletionSource();
            }
            await ObserveTasksAsync();
        }

        protected virtual async Task RollupStatesAsync(CancellationToken ct)
        {
            await DbStatus.RollupStatesAsync(
                _fromState,
                _toState,
                ct);
        }

        protected void EnqueueUnobservedTask(Task task, CancellationToken ct)
        {
            _unobservedTasksQueue.Enqueue(task);
        }

        private bool HasWorkableIterations()
        {
            return DbStatus.GetIterations().Any(i => i.State < _toState);
        }

        private async Task ObserveTasksAsync()
        {
            if (_unobservedTasksQueue.TryPeek(out var task))
            {
                if (task.IsCompleted)
                {
                    await task;
                    _unobservedTasksQueue.TryDequeue(out var _);
                    await ObserveTasksAsync();
                }
            }
        }
    }
}