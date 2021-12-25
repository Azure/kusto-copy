using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyServices
{
    internal class KustoOperationAwaiter
    {
        private readonly KustoClient _kustoClient;

        public KustoOperationAwaiter(KustoClient kustoClient)
        {
            _kustoClient = kustoClient;
        }

        public Task WaitForOperationCompletionAsync(Guid operationId)
        {
            throw new NotImplementedException();
        }
    }
}