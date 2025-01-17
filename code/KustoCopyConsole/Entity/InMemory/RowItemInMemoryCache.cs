﻿using KustoCopyConsole.Entity.RowItems;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class RowItemInMemoryCache
    {
        private readonly object _lock = new object();
        private volatile IImmutableDictionary<string, ActivityCache> _activityMap =
            ImmutableDictionary<string, ActivityCache>.Empty;

        public event EventHandler<RowItemBase>? RowItemAppended;

        public RowItemInMemoryCache(IEnumerable<RowItemBase> items)
        {
            lock (_lock)
            {
                foreach (var item in items)
                {
                    AppendItem(item);
                }
            }
        }

        public IImmutableDictionary<string, ActivityCache> ActivityMap
            => _activityMap;

        public IEnumerable<ActivityFlatHierarchy> GetActivityFlatHierarchy(
            Func<ActivityCache, bool> activityPredicate,
            Func<IterationCache, bool> iterationPredicate)
        {
            return ActivityMap
                .Values
                .Where(a => activityPredicate(a))
                .SelectMany(a => a.IterationMap.Values.Where(i => iterationPredicate(i)).Select(i => new
                {
                    Activity = a,
                    Iteration = i
                }))
                .SelectMany(o => o.Iteration.BlockMap.Values.Select(b => new ActivityFlatHierarchy(
                    o.Activity.RowItem,
                    o.Iteration.RowItem,
                    b.RowItem)));
        }

        public IEnumerable<RowItemBase> GetItems()
        {
            foreach (var activity in ActivityMap.Values)
            {
                yield return activity.RowItem;
                foreach (var iteration in activity.IterationMap.Values)
                {
                    yield return iteration.RowItem;
                    if (iteration.TempTable != null)
                    {
                        yield return iteration.TempTable;
                    }
                    foreach (var block in iteration.BlockMap.Values)
                    {
                        yield return block.RowItem;
                        foreach (var url in block.UrlMap.Values)
                        {
                            yield return url.RowItem;
                        }
                    }
                }
            }
        }

        public void AppendItem(RowItemBase item)
        {
            lock (_lock)
            {
                Interlocked.Exchange(ref _activityMap, AppendItemToCache(item));
            }
            OnRowItemAppended(item);
        }

        private void OnRowItemAppended(RowItemBase item)
        {
            if (RowItemAppended != null)
            {
                RowItemAppended(this, item);
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendItemToCache(
            RowItemBase item)
        {
            switch (item)
            {
                case ActivityRowItem a:
                    return AppendActivity(a);
                case IterationRowItem i:
                    return AppendIteration(i);
                case TempTableRowItem t:
                    return AppendTempTable(t);
                case BlockRowItem sb:
                    return AppendBlock(sb);
                case UrlRowItem url:
                    return AppendUrl(url);
                default:
                    throw new NotSupportedException(
                        $"Not supported row item type:  {item.GetType().Name}");
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendActivity(
            ActivityRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var activity = _activityMap[activityName];

                return _activityMap.SetItem(
                    activityName,
                    new ActivityCache(item, activity.IterationMap));
            }
            else
            {
                return _activityMap.Add(activityName, new ActivityCache(item));
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendIteration(
            IterationRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var table = _activityMap[activityName];

                if (table.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = table.IterationMap[item.IterationId];

                    return _activityMap.SetItem(
                        activityName,
                        table.AppendIteration(
                            new IterationCache(item, iteration.TempTable, iteration.BlockMap)));
                }
                else
                {
                    return _activityMap.SetItem(
                        activityName,
                        table.AppendIteration(new IterationCache(item)));
                }
            }
            else
            {
                throw new NotSupportedException("Activity should come before block in logs");
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendTempTable(TempTableRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var activity = _activityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    return _activityMap.SetItem(
                        activityName,
                        activity.AppendIteration(
                            new IterationCache(iteration.RowItem, item, iteration.BlockMap)));
                }
                else
                {
                    throw new NotSupportedException("Iteration should come before block in logs");
                }
            }
            else
            {
                throw new NotSupportedException("Activity should come before block in logs");
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendBlock(BlockRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var activity = _activityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    if (iteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var sourceBlock = iteration.BlockMap[item.BlockId];

                        return _activityMap.SetItem(
                            activityName,
                            activity.AppendIteration(
                                iteration.AppendBlock(
                                    new BlockCache(item, sourceBlock.UrlMap))));
                    }
                    else
                    {
                        return _activityMap.SetItem(
                            activityName,
                            activity.AppendIteration(
                                iteration.AppendBlock(new BlockCache(item))));
                    }
                }
                else
                {
                    throw new NotSupportedException("Iteration should come before block in logs");
                }
            }
            else
            {
                throw new NotSupportedException("Activity should come before block in logs");
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendUrl(UrlRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var activity = _activityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    if (iteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var block = iteration.BlockMap[item.BlockId];

                        return _activityMap.SetItem(
                            activityName,
                            activity.AppendIteration(
                                iteration.AppendBlock(
                                    block.AppendUrl(new UrlCache(item)))));
                    }
                    else
                    {
                        throw new NotSupportedException("Block should come before url in logs");
                    }
                }
                else
                {
                    throw new NotSupportedException("Iteration should come before block in logs");
                }
            }
            else
            {
                throw new NotSupportedException("Activity should come before iteration in logs");
            }
        }
    }
}