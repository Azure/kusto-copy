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

        public IEnumerable<RowItemBase> GetItems()
        {
            foreach (var sourceTable in ActivityMap.Values)
            {
                foreach (var sourceTableIteration in sourceTable.IterationMap.Values)
                {
                    yield return sourceTableIteration.RowItem;
                    foreach (var block in sourceTableIteration.BlockMap.Values)
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
                OnRowItemAppended(item);
            }
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
                case BlockRowItem sb:
                    return AppendSourceBlock(sb);
                case UrlRowItem url:
                    return AppendSourceUrl(url);
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
                            new IterationCache(item, iteration.BlockMap)));
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

        private IImmutableDictionary<string, ActivityCache> AppendSourceBlock(
            BlockRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var sourceTable = _activityMap[activityName];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];

                    if (sourceIteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var sourceBlock = sourceIteration.BlockMap[item.BlockId];

                        return _activityMap.SetItem(
                            activityName,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(
                                    new BlockCache(item, sourceBlock.UrlMap))));
                    }
                    else
                    {
                        return _activityMap.SetItem(
                            activityName,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(new BlockCache(item))));
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

        private IImmutableDictionary<string, ActivityCache> AppendSourceUrl(UrlRowItem item)
        {
            var activityName = item.ActivityName;

            if (_activityMap.ContainsKey(activityName))
            {
                var sourceTable = _activityMap[activityName];

                if (sourceTable.IterationMap.ContainsKey(item.IterationId))
                {
                    var sourceIteration = sourceTable.IterationMap[item.IterationId];

                    if (sourceIteration.BlockMap.ContainsKey(item.BlockId))
                    {
                        var block = sourceIteration.BlockMap[item.BlockId];

                        return _activityMap.SetItem(
                            activityName,
                            sourceTable.AppendIteration(
                                sourceIteration.AppendBlock(
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