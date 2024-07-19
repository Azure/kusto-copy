using KustoCopyConsole.Entity;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyConsole.Storage
{
    internal class RowItemGateway
    {
        private readonly IAppendStorage _appendStorage;
        private readonly Func<IImmutableList<RowItem>, IEnumerable<RowItem>> _compactFunc;

        public RowItemGateway(
            IAppendStorage appendStorage,
            Func<IImmutableList<RowItem>, IEnumerable<RowItem>> compactFunc)
        {
            _appendStorage = appendStorage;
            _compactFunc = compactFunc;
        }

        public async Task MigrateToLatestVersionAsync(CancellationToken ct)
        {
            var buffer = await _appendStorage.LoadAllAsync(ct);

            throw new NotImplementedException();
        }

        public Task AppendAsync(RowItem item, CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task AppendAtomicallyAsync(IEnumerable<RowItem> items, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}