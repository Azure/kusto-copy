using CommandLine;
using CommandLine.Text;
using KustoCopyConsole.Orchestrations;
using KustoCopyConsole.Parameters;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Reflection;
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
                var versionAttribute = typeof(Program)
                    .Assembly
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>();
                var version = versionAttribute == null
                    ? "<VERSION MISSING>"
                    : versionAttribute!.InformationalVersion;

                return version;
            }
        }

        internal static async Task<int> Main(string[] args)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo("en-US");

            Console.WriteLine();
            Console.WriteLine($"kusto-copy { AssemblyVersion }");

            //  Use CommandLineParser NuGet package to parse command line
            //  See https://github.com/commandlineparser/commandline
            var parser = new Parser(with =>
            {
                with.HelpWriter = null;
            });

            try
            {
                var result = parser.ParseArguments<CommandLineOptions>(args);

                await result
                    .WithNotParsed(errors => HandleParseError(result, errors))
                    .WithParsedAsync(RunOptionsAsync);

                return result.Tag == ParserResultType.Parsed
                    ? 0
                    : 1;
            }
            catch (CopyException ex)
            {
                DisplayCopyException(ex);

                return 1;
            }
            catch (Exception ex)
            {
                DisplayGenericException(ex);

                return 1;
            }
        }

        private static void DisplayCopyException(CopyException ex, string tab = "")
        {
            Trace.TraceError($"{tab}Error:  {ex.Message}");

            var copyInnerException = ex.InnerException as CopyException;

            if (copyInnerException != null)
            {
                DisplayCopyException(copyInnerException, tab + "  ");
            }
            if (ex.InnerException != null)
            {
                DisplayGenericException(ex.InnerException, tab + "  ");
            }
        }

        private static void DisplayGenericException(Exception ex, string tab = "")
        {
            Console.Error.WriteLine(
                $"{tab}Exception encountered:  {ex.GetType().FullName} ; {ex.Message}");
            Console.Error.WriteLine($"{tab}Stack trace:  {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                DisplayGenericException(ex.InnerException, tab + "  ");
            }
        }

        private static async Task RunOptionsAsync(CommandLineOptions options)
        {
            var parameterization = MainParameterization.Create(options);

            ConfigureTrace(options.Verbose);

            Trace.WriteLine("");
            Trace.WriteLine("Initialization...");

            await CopyOrchestration.CopyAsync(parameterization);
        }

        private static void ConfigureTrace(bool isVerbose)
        {
            var consoleListener = new TextWriterTraceListener(Console.Out)
            {
                Filter = new MultiFilter(
                    new EventTypeFilter(isVerbose ? SourceLevels.Information : SourceLevels.Warning),
                    new SourceFilter("kusto-copy"))
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