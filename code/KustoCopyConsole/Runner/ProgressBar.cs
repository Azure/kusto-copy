﻿using KustoCopyConsole.Entity.InMemory;
using KustoCopyConsole.Entity.RowItems.Keys;
using KustoCopyConsole.Entity.State;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Runner
{
    internal class ProgressBar : IAsyncDisposable
    {
        private static readonly TimeSpan WAKE_PERIOD = TimeSpan.FromSeconds(5);

        private readonly RowItemInMemoryCache _inMemoryCache;
        private readonly Task _backgroundTask;
        private readonly TaskCompletionSource _completionSource = new TaskCompletionSource();

        public ProgressBar(RowItemInMemoryCache inMemoryCache, CancellationToken ct)
        {
            _inMemoryCache = inMemoryCache;
            _backgroundTask = BackgroundRunAsync(ct);
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _completionSource.TrySetResult();
            await _backgroundTask;
        }

        private async Task BackgroundRunAsync(CancellationToken ct)
        {
            var iterationBag =
                (IImmutableSet<IterationKey>)ImmutableHashSet<IterationKey>.Empty;

            while (!_completionSource.Task.IsCompleted)
            {
                iterationBag = ReportProgress(iterationBag);
                await Task.WhenAny(
                    Task.Delay(WAKE_PERIOD, ct),
                    _completionSource.Task);
            }
        }

        private IImmutableSet<IterationKey> ReportProgress(
            IImmutableSet<IterationKey> iterationBag)
        {
            var activeIterations = _inMemoryCache
                .ActivityMap
                .Values
                .Where(a => a.RowItem.State == ActivityState.Active)
                .SelectMany(a => a.IterationMap.Values)
                .Where(i => i.RowItem.State != IterationState.Completed)
                .Select(i => i.RowItem.GetIterationKey())
                .ToImmutableHashSet();

            foreach (var key in iterationBag.Except(activeIterations))
            {
                Console.WriteLine(
                    $"Completed ({key.ActivityName}, {key.IterationId})");
            }
            foreach (var key in activeIterations)
            {
                ReportIterationProgress(key);
            }

            return activeIterations;
        }

        private void ReportIterationProgress(IterationKey key)
        {
            var iterationCache = _inMemoryCache
                .ActivityMap[key.ActivityName]
                .IterationMap[key.IterationId];
            var blockMap = iterationCache.BlockMap;
            var blockItems = blockMap.Values.Select(b => b.RowItem);
            var exportingCount = blockItems.Count(b => b.State == BlockState.Exporting);
            var exportedCount = blockItems.Count(b => b.State == BlockState.Exported);
            var queuedCount = blockItems.Count(b => b.State == BlockState.Queued);
            var ingestedCount = blockItems.Count(b => b.State == BlockState.Ingested);
            var movedCount = blockItems.Count(b => b.State == BlockState.ExtentMoved);

            Console.WriteLine(
                $"Progress ({key.ActivityName}, {key.IterationId}):  " +
                $"Planned={blockMap.Count}, " +
                $"Exporting={exportingCount}, Exported={exportedCount}, " +
                $"Queued={queuedCount}, Ingested={ingestedCount}, " +
                $"Moved={movedCount}");
        }
    }
}