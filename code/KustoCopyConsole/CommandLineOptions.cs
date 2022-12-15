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

        [Option('p', "parameter", Required = false, HelpText = "Set parameter file path.")]
        public string? ParameterFilePath { get; set; }

        [Option(
            "continuous",
            Required = false,
            HelpText = "Continuous run:  if set, runs continuously, otherwise, stop after first batch")]
        public bool ContinuousRun { get; set; } = false;

        [Option('l', "lake", Required = true, HelpText = "Data Lake (ADLS gen 2) folder URL or Kusto-style connection string")]
        public string? LakeFolderConnectionString { get; set; }

        [Option('s', "source", Required = false, HelpText = "Source Cluster Query Connection String")]
        public string SourceConnectionString { get; set; } = string.Empty;

        [Option('d', "destination", Required = false, HelpText = "Destination Cluster Query Connection String")]
        public string DestinationConnectionString { get; set; } = string.Empty;

        [Option("db", Required = false, HelpText = "Database to copy")]
        public string? Db { get; set; }

        [Option("tables-include", Required = false, HelpText = "Tables to include", Default = new string[0])]
        public IEnumerable<string> TablesToInclude { get; set; } = new string[0];

        [Option("tables-exclude", Required = false, HelpText = "Tables to exclude", Default = new string[0])]
        public IEnumerable<string> TablesToExclude { get; set; } = new string[0];

        [Option("query-slots", Required = false, HelpText = "Number of concurrent queries / commands on the clusters")]
        public int ConcurrentQueryCount { get; set; } = 1;

        [Option("export-slots", Required = false, HelpText = "# export slots to use on source cluster")]
        public int ConcurrentExportCommandCount { get; set; } = 0;

        [Option("ingestion-slots", Required = false, HelpText = "Number of concurrent ingestions on the clusters")]
        public int ConcurrentIngestionCount { get; set; } = 0;

        [Option('r', "rpo", Required = false, HelpText = "Recovery Point Objectives:  the target timespan between 2 iterations")]
        public string Rpo { get; set; } = "5m";

        [Option('b', "backfillHorizon", Required = false, HelpText = "Backfill horizon:  how long in the past should we start?")]
        public string? BackfillHorizon { get; set; }
    }
}