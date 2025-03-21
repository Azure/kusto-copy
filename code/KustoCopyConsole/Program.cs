﻿using CommandLine;
using CommandLine.Text;
using KustoCopyConsole.JobParameter;
using KustoCopyConsole.Runner;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

// See https://aka.ms/new-console-template for more information

namespace KustoCopyConsole
{
    internal class Program
    {
        #region Inner Types
        private class MultiFilter : TraceFilter
        {
            private readonly IImmutableList<TraceFilter> _filters;

            public MultiFilter(params TraceFilter[] filters)
            {
                _filters = filters.ToImmutableArray();
            }

            public override bool ShouldTrace(
                TraceEventCache? cache,
                string source,
                TraceEventType eventType,
                int id,
                string? formatOrMessage,
                object?[]? args,
                object? data1,
                object?[]? data)
            {
                foreach (var filter in _filters)
                {
                    if (!filter.ShouldTrace(
                        cache,
                        source,
                        eventType,
                        id,
                        formatOrMessage,
                        args,
                        data1,
                        data))
                    {
                        return false;
                    }
                }

                return true;
            }
        }
        #endregion

        public static string AssemblyVersion
        {
            get
            {
                var version = typeof(Program).Assembly.GetName().Version;
                var versionText = version == null
                    ? "<VERSION MISSING>"
                    : version.ToString();

                return versionText;
            }
        }

        //  This attributes is there to prevent an error when compiling with trimming
        //  since CommandLineOptions is accessed by reflection
        [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(CommandLineOptions))]
        internal static async Task<int> Main(string[] args)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");

            Console.WriteLine();
            Console.WriteLine($"Kusto Copy {AssemblyVersion}");
            Console.WriteLine();

            //  Use CommandLineParser NuGet package to parse command line
            //  See https://github.com/commandlineparser/commandline
            var parser = new Parser(with =>
            {
                with.HelpWriter = null;
            });

            try
            {
                var options = parser.ParseArguments<CommandLineOptions>(args);

                await options
                    .WithNotParsed(errors => HandleParseError(options, errors))
                    .WithParsedAsync(RunOptionsAsync);

                return options.Tag == ParserResultType.Parsed
                    ? 0
                    : 1;
            }
            catch (Exception ex)
            {
                ErrorHelper.DisplayException(ex);

                return 1;
            }
        }

        private static async Task RunOptionsAsync(CommandLineOptions options)
        {
            ConfigureTrace(options.Verbose);

            var cancellationTokenSource = new CancellationTokenSource();
            var taskCompletionSource = new TaskCompletionSource();
            var parameterization = MainJobParameterization.FromOptions(options);

            AppDomain.CurrentDomain.ProcessExit += (e, s) =>
            {
                Trace.TraceInformation("Exiting process...");
                cancellationTokenSource.Cancel();
                taskCompletionSource.Task.Wait();
            };
            Trace.WriteLine("");
            Trace.WriteLine("Parameterization:");
            Trace.WriteLine("");
            Trace.WriteLine(parameterization.ToYaml());
            Trace.WriteLine("");
            try
            {
                await using (var mainRunner = await MainRunner.CreateAsync(
                    new Version(AssemblyVersion),
                    parameterization,
                    $"KUSTO-COPY;{AssemblyVersion}",
                    cancellationTokenSource.Token))
                {
                    Trace.WriteLine("Processing...");
                    Trace.WriteLine("");
                    await mainRunner.RunAsync(cancellationTokenSource.Token);
                }
            }
            catch(Exception)
            {
                await cancellationTokenSource.CancelAsync();
                throw;
            }
            finally
            {
                taskCompletionSource.SetResult();
            }
        }

        private static void ConfigureTrace(bool isVerbose)
        {
            var consoleListener = new TextWriterTraceListener(Console.Out)
            {
                Filter = new EventTypeFilter(
                    isVerbose
                    ? SourceLevels.Information
                    : SourceLevels.Warning)
            };

            Trace.Listeners.Add(consoleListener);
            if (isVerbose)
            {
                Trace.TraceInformation("Verbose output enabled");
            }
        }

        private static void HandleParseError(
            ParserResult<CommandLineOptions> result,
            IEnumerable<Error> errors)
        {
            var helpText = HelpText.AutoBuild(result, h =>
            {
                h.AutoVersion = false;
                h.Copyright = string.Empty;
                h.Heading = string.Empty;

                return HelpText.DefaultParsingErrorsHandler(result, h);
            }, example => example);

            Console.WriteLine(helpText);
        }
    }
}