using CommandLine;
using KustoCopyConsole.JobParameter;
using System;
using System.Collections.Generic;
using System.Text;

namespace KustoCopyConsole
{
    public class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('y', "yaml", Required = false, HelpText = "Set YAML path description file")]
        public string Yaml { get; set; } = string.Empty;

        [Option(
            's',
            "source",
            Required = false,
            HelpText = "Set the source in the form cluster uri/database/table, e.g. https://help.kusto.windows.net/Samples/nyc_taxi")]
        public string Source { get; set; } = string.Empty;

        [Option(
            'd',
            "destination",
            Required = false,
            HelpText = "Set the destination table in the form cluster uri/database/table (table is optional), e.g. https://mycluster.eastus.kusto.windows.net/mydb")]
        public string Destination { get; set; } = string.Empty;

        [Option(
            't',
            "staging-storage",
            Required = false,
            HelpText = "Set the staging storage directories in the form of storage container uris")]
        public IEnumerable<string> StagingStorageDirectories { get; set; } = Array.Empty<string>();

        [Option("clientId", Required = false, HelpText = "Managed Identity client ID")]
        public string ManagedIdentityClientId { get; set; } = string.Empty;

        [Option('q', "query", Required = false, HelpText = "Set query, e.g. nyc_taxi.")]
        public string Query { get; set; } = string.Empty;

        [Option(
            "export",
            Required = false,
            HelpText = "Parallel export count")]
        public int? ExportCount { get; set; }

        [Option(
            "copy-mode",
            Required = false,
            HelpText = "Copy mode:  BackfillOnly, NewOnly or BackfillAndNew")]
        public CopyMode? CopyMode { get; set; }

        [Option(
            "iteration-period",
            Required = false,
            HelpText = "Iteration period:  time between iterations")]
        public TimeSpan? IterationPeriod { get; set; }
    }
}