using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Export;
using KustoCopyBookmarks.Parameters;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.Intrinsics.Arm;

namespace KustoCopyServices
{
    internal class DbExportExecutionPipeline
    {
        #region Inner Types
        public class TablePlanContext : IComparable<TablePlanContext>
        {
            public TablePlanContext(
                DbEpochData dbEpoch,
                DbIterationData dbIteration,
                TableExportPlanData tablePlan)
            {
                DbEpoch = dbEpoch;
                DbIteration = dbIteration;
                TablePlan = tablePlan;
            }

            public DbEpochData DbEpoch { get; }

            public DbIterationData DbIteration { get; }

            public TableExportPlanData TablePlan { get; }

            public bool IsEmpty => !TablePlan.Steps.Any();

            int IComparable<TablePlanContext>.CompareTo(TablePlanContext? other)
            {
                if (other == null)
                {
                    throw new ArgumentNullException(nameof(other));
                }

                if (IsEmpty && other.IsEmpty)
                {
                    return 0;
                }
                else if (IsEmpty && !other.IsEmpty)
                {
                    return -1;
                }
                else if (!IsEmpty && other.IsEmpty)
                {
                    return 1;
                }
                else
                {
                    var thisIngestionTime = TablePlan.Steps.First().IngestionTimes.Max();
                    var otherIngestionTime = other.TablePlan.Steps.First().IngestionTimes.Max();

                    return thisIngestionTime.CompareTo(otherIngestionTime);
                }
            }
        }
        #endregion

        private readonly DbExportPlanBookmark _dbExportPlanBookmark;
        private readonly KustoQueuedClient _kustoClient;
        private readonly KustoExportQueue _exportQueue;
        private readonly PriorityQueue<TablePlanContext, TablePlanContext> _planQueue
            = new PriorityQueue<TablePlanContext, TablePlanContext>();
        //private readonly ConcurrentDictionary<DateTime, Bookmark> ;

        public DbExportExecutionPipeline(
            string dbName,
            DbExportPlanBookmark dbExportPlanBookmark,
            KustoQueuedClient kustoClient,
            double exportSlotsRatio)
        {
            DbName = dbName;
            _dbExportPlanBookmark = dbExportPlanBookmark;
            _kustoClient = kustoClient;
            _exportQueue = new KustoExportQueue(_kustoClient, exportSlotsRatio);
            //  Populate plan queue
            var list = new List<TablePlanContext>();

            foreach (var dbEpoch in _dbExportPlanBookmark.GetAllDbEpochs())
            {
                foreach (var dbIteration in _dbExportPlanBookmark.GetDbIterations(dbEpoch.EndCursor))
                {
                    var plans = _dbExportPlanBookmark.GetTableExportPlans(
                        dbIteration.EpochEndCursor,
                        dbIteration.Iteration);

                    foreach (var tablePlan in plans)
                    {
                        var context = new TablePlanContext(dbEpoch, dbIteration, tablePlan);

                        list.Add(context);
                    }
                }
            }
            _planQueue.EnqueueRange(list.Select(c => (c, c)));
            _dbExportPlanBookmark.NewDbIteration += (sender, e) =>
            {
                lock (_planQueue)
                {
                    var contexts = e.TablePlans
                    .Select(p => new TablePlanContext(e.DbEpoch, e.DbIteration, p))
                    .Select(c => (c, c));

                    _planQueue.EnqueueRange(contexts);
                }
            };
        }

        public string DbName { get; }

        public async Task RunAsync()
        {
            var stopGoAwaiter = new StopGoAwaiter(false);
            var taskList = new List<Task>();

            _dbExportPlanBookmark.NewDbIteration += (sender, e) =>
            {   //  Make sure we wake up
                stopGoAwaiter.Go();
            };

            while (true)
            {
                TablePlanContext? context;

                lock (_planQueue)
                {
                    if (!_planQueue.TryDequeue(out context, out _))
                    {
                        stopGoAwaiter.Stop();
                    }
                }
                if (context == null)
                {
                    await stopGoAwaiter.WaitForGoAsync();
                }
                else
                {
                    while (!_exportQueue.HasAvailability)
                    {
                        taskList = await CleanTaskListAsync(taskList);
                    }
                    taskList.Add(ProcessPlanAsync(context));
                }
            }
        }

        private Task ProcessPlanAsync(TablePlanContext context)
        {
            throw new NotImplementedException();
        }

        private static async Task<List<Task>> CleanTaskListAsync(List<Task> taskList)
        {
            await Task.WhenAny(taskList);

            //  We take a snapshot of the state
            var snapshot = taskList
                .Select(t => new
                {
                    t.IsCompleted,
                    Task = t
                })
                .ToImmutableArray();
            var completed = snapshot
                .Where(s => s.IsCompleted);

            foreach (var s in completed)
            {
                await s.Task;
            }

            return snapshot
                .Where(s => !s.IsCompleted)
                .Select(s => s.Task)
                .ToList();
        }
    }
}