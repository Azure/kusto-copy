using Kusto.Cloud.Platform.Data;
using Kusto.Data.Common;
using KustoCopyConsole.Concurrency;
using KustoCopyConsole.Entity;
using KustoCopyConsole.Kusto.Data;
using System.Collections.Immutable;
using System.Data;

namespace KustoCopyConsole.Kusto
{
    internal class DmCommandClient
    {
        private readonly ICslAdminProvider _provider;
        private readonly PriorityExecutionQueue<KustoPriority> _queue;
        private readonly string _databaseName;

        public DmCommandClient(
            ICslAdminProvider provider,
            PriorityExecutionQueue<KustoPriority> queue,
            string databaseName)
        {
            _provider = provider;
            _queue = queue;
            _databaseName = databaseName;
        }

        public async Task<IImmutableList<Uri>> GetTempStorageUrisAsync(CancellationToken ct)
        {
            return await _queue.RequestRunAsync(
                KustoPriority.HighestPriority,
                async () =>
                {
                    var commandText = ".get ingestion resources";
                    var reader = await _provider.ExecuteControlCommandAsync(
                        string.Empty,
                        commandText);
                    var storageRoots = reader
                        .ToEnumerable(r => new
                        {
                            ResourceTypeName = (string)r["ResourceTypeName"],
                            StorageRoot = (string)r["StorageRoot"]
                        })
                        .Where(o => o.ResourceTypeName == "TempStorage")
                        .Select(o => new Uri(o.StorageRoot))
                        .ToImmutableArray();

                    return storageRoots;
                });
        }
    }
}