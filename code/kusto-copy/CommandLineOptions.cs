using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace kusto_copy
{
    internal class CommandLineOptions
    {
        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('l', "lake", Required = true, HelpText = "Data Lake (ADLS gen 2) folder URL")]
        public string Lake { get; set; } = string.Empty;

        [Option('s', "source", Required = false, HelpText = "Source Cluster URI")]
        public string Source { get; set; } = string.Empty;

        [Option('p', "parameter", Required = false, HelpText = "Set parameter file path.")]
        public string ParameterFilePath { get; set; } = string.Empty;
    }
}