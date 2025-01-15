using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Concurrency
{
    internal class AsyncCache<T>
    {
        #region Inner Types
        private record CacheNode(
            TaskCompletionSource EnterSource,
            TaskCompletionSource ExitSource,
            bool IsItemAvailable,
            T? Item,
            DateTime ExpirationTime);
        #endregion

        private readonly Func<Task<(TimeSpan, T)>> _asyncFetchFunction;
        private volatile CacheNode _cacheNode = new CacheNode(
            new TaskCompletionSource(),
            new TaskCompletionSource(),
            false,
            (T?)default,
            DateTime.Now);

        public AsyncCache(Func<Task<(TimeSpan, T)>> asyncFetchFunction)
        {
            _asyncFetchFunction = asyncFetchFunction;
        }

        public async Task<T> GetCacheItemAsync(CancellationToken ct)
        {
            while (true)
            {
                var cacheNode = _cacheNode;

                if (cacheNode.IsItemAvailable && cacheNode.ExpirationTime > DateTime.Now)
                {
                    return cacheNode.Item!;
                }
                else
                {   //  Try to "enter" the cache node:  this might be a competition
                    if (cacheNode.EnterSource.TrySetResult())
                    {
                        var result = await _asyncFetchFunction();
                        var newCacheNode = new CacheNode(
                            new TaskCompletionSource(),
                            new TaskCompletionSource(),
                            true,
                            result.Item2,
                            DateTime.Now.Add(result.Item1));

                        Interlocked.Exchange(ref _cacheNode, newCacheNode);
                        cacheNode.ExitSource.SetResult();

                        return newCacheNode.Item!;
                    }
                    else
                    {   //  Fail to enter the cache node:  another thread will fetch the item
                        await cacheNode.ExitSource.Task;
                    }
                }
            }
        }
    }
}