using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole
{
    internal class BackgroundTaskContainer
    {
        private readonly List<Task> _tasks = new();

        public void AddTask(Task task)
        {
            lock (_tasks)
            {
                _tasks.Add(task);
            }
        }

        public async Task ObserveCompletedTasksAsync(CancellationToken ct)
        {
            var completedTasks = RemoveCompletedTasks();

            await Task.WhenAll(completedTasks);
        }

        private IEnumerable<Task> RemoveCompletedTasks()
        {
            lock (_tasks)
            {
                var snapshots = _tasks
                    .Select(t => new
                    {
                        Task = t,
                        t.IsCompleted
                    })
                    .ToImmutableArray();
                var completedTasks = snapshots
                    .Where(o => o.IsCompleted)
                    .Select(o => o.Task);
                var incompletedTasks = snapshots
                    .Where(o => !o.IsCompleted)
                    .Select(o => o.Task);

                _tasks.Clear();
                _tasks.AddRange(incompletedTasks);

                return completedTasks;
            }
        }
    }
}