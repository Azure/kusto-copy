using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;
using KustoCopyConsole.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Orchestration
{
    internal abstract class SubOrchestrationBase
    {
        protected SubOrchestrationBase(
            RowItemGateway rowItemGateway,
            DbClientFactory dbClientFactory,
            MainJobParameterization parameterization)
        {
            RowItemGateway = rowItemGateway;
            DbClientFactory = dbClientFactory;
            Parameterization = parameterization;
        }

        protected BackgroundTaskContainer BackgroundTaskContainer { get; } = new();

        protected RowItemGateway RowItemGateway { get; }

        protected DbClientFactory DbClientFactory { get; }

        protected MainJobParameterization Parameterization { get; }

        public async Task ProcessAsync(Task drivingTask, CancellationToken ct)
        {
            EventHandler<RowItemAppend> rowItemAppendedHandler = (sender, e) =>
            {
                OnProcessRowItemAppended(e, ct);
            };

            RowItemGateway.RowItemAppended += rowItemAppendedHandler;
            try
            {
                var processTask = OnStartProcessAsync(ct);

                while (!drivingTask.IsCompleted)
                {
                    await Task.WhenAny(
                        drivingTask,
                        Task.Delay(TimeSpan.FromSeconds(10), ct));
                    if (processTask.IsCompleted)
                    {   //  Surface errors if there are
                        await processTask;
                    }
                    await BackgroundTaskContainer.ObserveCompletedTasksAsync(ct);
                }
                await processTask;
                await BackgroundTaskContainer.ObserveCompletedTasksAsync(ct);
            }
            finally
            {
                RowItemGateway.RowItemAppended -= rowItemAppendedHandler;
            }
        }

        protected abstract Task OnStartProcessAsync(CancellationToken ct);

        protected virtual void OnProcessRowItemAppended(RowItemAppend e, CancellationToken ct)
        {
        }
    }
}