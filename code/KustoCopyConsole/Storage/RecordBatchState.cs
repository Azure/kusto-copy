using Kusto.Cloud.Platform.Utils;
using System.Collections.Immutable;

namespace KustoCopyConsole.Storage
{
    public class RecordBatchState
    {
        public PlanRecordBatchState? PlanRecordBatchState { get; set; }
        
        public ExportRecordBatchState? ExportRecordBatchState { get; set; }
        
        public StageRecordBatchState? StageRecordBatchState { get; set; }
    }
}