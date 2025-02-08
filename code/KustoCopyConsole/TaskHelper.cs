using System.Collections.Immutable;

namespace KustoCopyConsole
{
    internal static class TaskHelper
    {
        public static async Task WhenAllWithErrors(params IEnumerable<Task> tasks)
        {
            var remainingTasks = tasks.ToImmutableArray();

            while (remainingTasks.Any())
            {
                await Task.WhenAny(remainingTasks);

                var isCompletedSnapshot = remainingTasks
                    .Select(t => t.IsCompleted)
                    .ToImmutableArray();
                var zipped = remainingTasks
                    .Zip(isCompletedSnapshot, (t, i) => new
                    {
                        Task = t,
                        IsCompleted = i
                    });
                var completedTasks = zipped
                    .Where(z => z.IsCompleted)
                    .Select(z => z.Task);
                var incompletedTasks = zipped
                    .Where(z => !z.IsCompleted)
                    .Select(z => z.Task);

                //  This will raise errors if need be
                await Task.WhenAll(completedTasks);
                //  Continue with incompleted tasks
                remainingTasks = incompletedTasks.ToImmutableArray();
            }
        }
    }
}