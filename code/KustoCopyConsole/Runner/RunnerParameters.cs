using Azure.Core;
using KustoCopyConsole.Entity;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Kusto;

namespace KustoCopyConsole.Runner
{
    internal record RunnerParameters(
        MainJobParameterization Parameterization,
        TokenCredential Credential,
        TrackDatabase Database,
        DbClientFactory DbClientFactory,
        AzureBlobUriProvider StagingBlobUriProvider);
}