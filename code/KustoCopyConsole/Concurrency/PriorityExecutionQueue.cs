using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    public class PriorityExecutionQueue<TPriority> : IAsyncDisposable
    {
        #region Inner Types
        private abstract class Request
        {
            protected Request(TPriority priority)
            {
                Priority = priority;
            }

            public TPriority Priority { get; }

            public abstract Task ExecuteAsync();
        }

        private class Request<T> : Request
        {
            private readonly Func<Task<T>> _asyncAction;

            public Request(TPriority priority, Func<Task<T>> asyncAction)
                : base(priority)
            {
                _asyncAction = asyncAction;
            }

            public TaskCompletionSource<T> Source { get; } = new TaskCompletionSource<T>();

            public override async Task ExecuteAsync()
            {
                var value = await _asyncAction();

                Source.SetResult(value);
            }
        }
        #endregion

        private readonly PriorityQueue<Request, TPriority> _requestQueue = new();
        private readonly List<Task> _runningRequestTasks = new();
        private readonly Channel<Request> _channel = Channel.CreateUnbounded<Request>();
        private readonly TaskCompletionSource _stopSource = new TaskCompletionSource();
        private readonly Task _backgroundTask;

        public PriorityExecutionQueue(int maxParallelRunCount)
        {
            if (maxParallelRunCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(maxParallelRunCount));
            }
            MaxParallelRunCount = maxParallelRunCount;
            _backgroundTask = RunBackgroundTask();
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _stopSource.TrySetResult();

            await _backgroundTask;
        }

        public int MaxParallelRunCount { get; }

        public async Task<T> RequestRunAsync<T>(TPriority priority, Func<Task<T>> actionAsync)
        {
            var request = new Request<T>(priority, actionAsync);

            await ObserveRunnerTasksAsync();
            if (!_channel.Writer.TryWrite(request))
            {
                throw new InvalidOperationException("Couldn't push the request");
            }

            return await request.Source.Task;
        }

        public async Task RequestRunAsync(TPriority priority, Func<Task> actionAsync)
        {
            await RequestRunAsync(priority, async () =>
            {
                await actionAsync();

                return 0;
            });
        }

        private async Task ObserveRunnerTasksAsync()
        {
            if (_backgroundTask.Status == TaskStatus.Faulted)
            {
                await _backgroundTask;
            }
        }

        private async Task RunBackgroundTask()
        {
            while (!_stopSource.Task.IsCompleted)
            {
                var requestTask = _channel.Reader.WaitToReadAsync().AsTask();

                //  Wait for a request / task
                await Task.WhenAny(_runningRequestTasks.Append(requestTask).Append(_stopSource.Task));

                //  Ensure the request won
                if (!_stopSource.Task.IsCompleted)
                {
                    if (requestTask.IsCompleted)
                    {
                        QueueRequests();
                    }
                    else
                    {
                        await ObserveCompletedRequestsAsync();
                    }
                    RunRequests();
                }
            }
        }

        private void QueueRequests()
        {
            while (_channel.Reader.TryRead(out var request))
            {
                _requestQueue.Enqueue(request, request.Priority);
            }
        }

        private async Task ObserveCompletedRequestsAsync()
        {
            var completedRequests = _runningRequestTasks
                .Index()
                .Select(p => new
                {
                    p.Index,
                    Task = p.Item
                })
                .Where(o => o.Task.IsCompleted)
                .ToImmutableArray();
            //  Sort in reverse so we can remove them
            var completedIndexes = completedRequests
                .Select(o => o.Index)
                .OrderByDescending(i => i);

            //  Observe
            await Task.WhenAll(completedRequests.Select(o => o.Task));
            //  Clean task list
            foreach (var index in completedIndexes)
            {
                _runningRequestTasks.RemoveAt(index);
            }
        }

        private void RunRequests()
        {
            while (_runningRequestTasks.Count < MaxParallelRunCount && _requestQueue.Count > 0)
            {
                var request = _requestQueue.Dequeue();

                _runningRequestTasks.Add(Task.Run(() => request.ExecuteAsync()));
            }
        }
    }
}