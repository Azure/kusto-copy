using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace KustoCopyConsole
{
    public class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option(
            's',
            "source",
            Required = false,
            HelpText = "Set the source in the format cluster uri/database/table, e.g. https://help.kusto.windows.net/Samples/nyc_taxi")]
        public string Source { get; set; }

        [Option(
            'd',
            "destination",
            Required = false,
            HelpText = "Set the destination database in the format uri cluster uri/database, e.g. https://vppartitioning.eastus.kusto.windows.net/mydb")]
        public string Destination { get; set; }

        [Option('a', "auth", Required = false, HelpText = "Set authentication method.")]
        public string Authentication { get; set; } = "AzCli";

        [Option(
            'l',
            "storage",
            Required = true,
            HelpText = "Set ADLS gen 2 storage account URLs (at least one), separated by commas")]
        public string StorageUrls { get; set; }

        [Option(
            "continuous",
            Required = false,
            HelpText = "Continuous run:  if set, runs continuously, otherwise, stop after one iteration")]
        public bool ContinuousRun { get; set; } = false;

        [Option("job-name", Required = false, HelpText = "Set job name.")]
        public string JobName { get; set; } = "default";

        [Option("job-path", Required = false, HelpText = "Set job file local path.")]
        public string JobFilePath { get; set; }
    }
}