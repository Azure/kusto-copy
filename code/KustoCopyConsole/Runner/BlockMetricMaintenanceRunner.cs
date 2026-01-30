using KustoCopyConsole.Entity;
using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Immutable;
using System.Linq;

namespace KustoCopyConsole.Runner
{
    internal class BlockMetricMaintenanceRunner : RunnerBase
    {
        public BlockMetricMaintenanceRunner(RunnerParameters parameters)
           : base(parameters, TimeSpan.FromSeconds(5))
        {
        }

        public async Task RunAsync(CancellationToken ct)
        {
            while (!AllActivitiesCompleted())
            {
                MaintainBlockMetrics();

                await SleepAsync(ct);
            }
        }

        private void MaintainBlockMetrics()
        {
            using (var tx = Database.CreateTransaction())
            {
                var blockMetricMap = Database.BlockMetrics.Query(tx)
                    //  Group by iteration and metric
                    .GroupBy(bm => new
                    {
                        bm.IterationKey,
                        bm.BlockMetric
                    })
                    //  Keep only those iteration / metric having more than one duplicates
                    .Where(g => g.Count() > 1)
                    .ToImmutableDictionary(g => g.Key, g => g.Sum(bm => bm.Value));

                foreach (var p in blockMetricMap)
                {
                    var aggregatedValue = p.Value;

#if DEBUG
                    if (aggregatedValue < 0)
                    {
                        throw new InvalidOperationException(
                            $"Value of {aggregatedValue} for metric '{p.Key.BlockMetric}'");
                    }
#endif
                    //  Delete duplicates
                    Database.BlockMetrics.Query(tx)
                        .Where(pf => pf.Equal(bm => bm.IterationKey, p.Key.IterationKey))
                        .Where(pf => pf.Equal(bm => bm.BlockMetric, p.Key.BlockMetric))
                        .Delete();
                    Database.BlockMetrics.AppendRecord(
                        new BlockMetricRecord(p.Key.IterationKey, p.Key.BlockMetric, aggregatedValue),
                        tx);
                }

                tx.Complete();
            }
        }
    }
}