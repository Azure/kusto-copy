using KustoCopyConsole.Entity.RowItems;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.InMemory
{
    internal class RowItemInMemoryCache
    {
        #region Constructors
        private RowItemInMemoryCache(IImmutableDictionary<string, ActivityCache> activityMap)
        {
            ActivityMap = activityMap;
        }

        public RowItemInMemoryCache(IEnumerable<RowItemBase> items)
            : this(ImmutableDictionary<string, ActivityCache>.Empty)
        {
            foreach (var item in items)
            {
                ActivityMap = AppendItemToActivityCache(item);
            }
        }
        #endregion

        public IImmutableDictionary<string, ActivityCache> ActivityMap { get; }

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
                    Iteration = i,
                    TempTableItem = i.TempTable
                }))
                .SelectMany(o => o.Iteration.BlockMap.Values.Select(b => new ActivityFlatHierarchy(
                    o.Activity.RowItem,
                    o.Iteration.RowItem,
                    o.TempTableItem,
                    b.RowItem,
                    b.UrlMap.Values.Select(u => u.RowItem))));
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

        public RowItemInMemoryCache CleanOnRestart()
        {
            var newActivityMap = ActivityMap.Values
                .Select(a => a.CleanOnRestart())
                .ToImmutableDictionary(a => a.RowItem.ActivityName);

            return new RowItemInMemoryCache(newActivityMap);
        }

        public RowItemInMemoryCache AppendItem(RowItemBase item)
        {
            return new RowItemInMemoryCache(AppendItemToActivityCache(item));
        }

        private IImmutableDictionary<string, ActivityCache> AppendItemToActivityCache(
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

            if (ActivityMap.ContainsKey(activityName))
            {
                var activity = ActivityMap[activityName];

                return ActivityMap.SetItem(
                    activityName,
                    new ActivityCache(item, activity.IterationMap));
            }
            else
            {
                return ActivityMap.Add(activityName, new ActivityCache(item));
            }
        }

        private IImmutableDictionary<string, ActivityCache> AppendIteration(
            IterationRowItem item)
        {
            var activityName = item.ActivityName;

            if (ActivityMap.ContainsKey(activityName))
            {
                var table = ActivityMap[activityName];

                if (table.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = table.IterationMap[item.IterationId];

                    return ActivityMap.SetItem(
                        activityName,
                        table.AppendIteration(
                            new IterationCache(item, iteration.TempTable, iteration.BlockMap)));
                }
                else
                {
                    return ActivityMap.SetItem(
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

            if (ActivityMap.ContainsKey(activityName))
            {
                var activity = ActivityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    return ActivityMap.SetItem(
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

            if (ActivityMap.ContainsKey(activityName))
            {
                var activity = ActivityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    if (iteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var block = iteration.BlockMap[item.BlockId];

                        return ActivityMap.SetItem(
                            activityName,
                            activity.AppendIteration(
                                iteration.AppendBlock(
                                    new BlockCache(item, block.UrlMap))));
                    }
                    else
                    {
                        return ActivityMap.SetItem(
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

            if (ActivityMap.ContainsKey(activityName))
            {
                var activity = ActivityMap[activityName];

                if (activity.IterationMap.ContainsKey(item.IterationId))
                {
                    var iteration = activity.IterationMap[item.IterationId];

                    if (iteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var block = iteration.BlockMap[item.BlockId];
                        var na = ActivityMap.SetItem(
                            activityName,
                            activity.AppendIteration(
                                iteration.AppendBlock(
                                    block.AppendUrl(new UrlCache(item)))));
                        var ni = activity.AppendIteration(
                                iteration.AppendBlock(
                                    block.AppendUrl(new UrlCache(item))));
                        var nii = iteration.AppendBlock(
                                    block.AppendUrl(new UrlCache(item)));

                        return ActivityMap.SetItem(
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