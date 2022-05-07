﻿using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace kusto_copy
{
    internal class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('a', "authMode", Required = false, HelpText = "Authentication mode:  AppSecret, AzCli or Browser")]
        public AuthenticationMode AuthenticationMode { get; set; } = AuthenticationMode.AzCli;

        [Option('l', "lake", Required = true, HelpText = "Data Lake (ADLS gen 2) folder URL")]
        public string Lake { get; set; } = string.Empty;

        [Option('p', "parameter", Required = false, HelpText = "Set parameter file path.")]
        public string? ParameterFilePath { get; set; }

        //[Option("dbs", Required = false, Separator = ',', HelpText = "DBs (from the source) to include")]
        //public string[] Dbs { get; set; } = new string[0];

        [Option('s', "source", Required = false, HelpText = "Source Cluster URI")]
        public string Source { get; set; } = string.Empty;

        [Option('q', "concurrent-query", Required = false, HelpText = "Number of concurrent queries / commands per cluster")]
        public int? ConcurrentQueries { get; set; } = null;

        [Option('e', "export-slots", Required = false, HelpText = "# export slots to use")]
        public int? ConcurrentExportCommandCount { get; set; }

        [Option('t', "tenant", Required = false, HelpText = "AAD Tenant ID")]
        public string? Tenant { get; set; }

        [Option('i', "app-id", Required = false, HelpText = "Application (Client) ID")]
        public string? AppId { get; set; }

        [Option('c', "app-secret", Required = false, HelpText = "Application Secret (credential)")]
        public string? AppSecret { get; set; }
    }
}