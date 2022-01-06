using Azure.Core;
using Azure.Storage.Files.DataLake;
using Kusto.Data.Common;
using KustoCopyBookmarks;
using KustoCopyBookmarks.Common;
using KustoCopyBookmarks.Export;
using KustoCopyBookmarks.Parameters;
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

        private readonly DbExportBookmark _dbExportBookmark;
        private readonly KustoQueuedClient _kustoClient;
        private readonly PriorityQueue<TablePlanContext, TablePlanContext> _planQueue
            = new PriorityQueue<TablePlanContext, TablePlanContext>();

        public DbExportExecutionPipeline(
            string dbName,
            DbExportBookmark dbExportBookmark,
            KustoQueuedClient kustoClient)
        {
            DbName = dbName;
            _dbExportBookmark = dbExportBookmark;
            _kustoClient = kustoClient;
            //  Populate plan queue
            var list = new List<TablePlanContext>();

            foreach (var dbEpoch in _dbExportBookmark.GetAllDbEpochs())
            {
                foreach (var dbIteration in _dbExportBookmark.GetDbIterations(dbEpoch.EndCursor))
                {
                    var plans = _dbExportBookmark.GetTableExportPlans(
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
            _dbExportBookmark.NewDbIteration += (sender, e) =>
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

            _dbExportBookmark.NewDbIteration += (sender, e) =>
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
            }
        }
    }
}