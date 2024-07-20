using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    internal class SourceDatabaseOrchestration : SubOrchestrationBase
    {
        private readonly SourceClusterParameterization _sourceCluster;
        private readonly SourceDatabaseParameterization _sourceDb;
        private readonly List<SourceDatabaseEntity> _sourceDatabaseEntities;

        public SourceDatabaseOrchestration(
            RowItemGateway rowItemGateway,
            SourceClusterParameterization sourceCluster,
            SourceDatabaseParameterization sourceDb,
            IEnumerable<RowItem> items)
            : base(rowItemGateway)
        {
            _sourceCluster = sourceCluster;
            _sourceDb = sourceDb;
            _sourceDatabaseEntities = items
                .Select(i => SourceDatabaseEntity.Create(i))
                .Where(e => e != null)
                .Select(e => e!)
                .ToList();
        }
    }
}