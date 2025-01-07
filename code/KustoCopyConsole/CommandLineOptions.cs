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
        public string Source { get; set; } = string.Empty;

        [Option(
            'd',
            "destination",
            Required = false,
            HelpText = "Set the destination in the format uri cluster uri/database/table (table is optional), e.g. https://mycluster.eastus.kusto.windows.net/mydb")]
        public string Destination { get; set; } = string.Empty;

        [Option('a', "auth", Required = false, HelpText = "Set authentication method.")]
        public string Authentication { get; set; } = string.Empty;

        [Option(
            "continuous",
            Required = false,
            HelpText = "Continuous run:  if set, runs continuously, otherwise, stop after one iteration")]
        public bool IsContinuousRun { get; set; } = false;

        [Option('q', "query", Required = false, HelpText = "Set query.")]
        public string Query { get; set; } = string.Empty;

        [Option("log-path", Required = false, HelpText = "Set log file path.")]
        public string LogFilePath { get; set; } = string.Empty;
    }
}