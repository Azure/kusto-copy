using CommandLine;
using CommandLine.Text;
using KustoCopyConsole.JobParameters;
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
            ConfigureTrace(options.Verbose);

            var parameterization = CreateParameterization(options);
            var cancellationTokenSource = new CancellationTokenSource();
            var taskCompletionSource = new TaskCompletionSource();

            Trace.WriteLine("");
            Trace.WriteLine("Parameterization:");
            Trace.WriteLine("");
            Trace.WriteLine(parameterization.ToYaml());
            Trace.WriteLine("");
            AppDomain.CurrentDomain.ProcessExit += (e, s) =>
            {
                Trace.TraceInformation("Exiting process...");
                cancellationTokenSource.Cancel();
                taskCompletionSource.Task.Wait();
            };

            try
            {
                Trace.WriteLine("Processing...");
                Trace.WriteLine("");
                parameterization.Validate();
                await Task.CompletedTask;
                //await CopyOrchestration.CopyAsync(
                //    parameterization,
                //    cancellationTokenSource.Token);
            }
            finally
            {
                taskCompletionSource.SetResult();
            }
        }

        private static MainJobParameterization CreateParameterization(CommandLineOptions options)
        {
            if (!string.IsNullOrWhiteSpace(options.Source))
            {
                if (string.IsNullOrWhiteSpace(options.Destination))
                {
                    throw new CopyException(
                        $"Source is specified ('options.Source'):  destination is expected",
                        false);
                }

                if (!Uri.TryCreate(options.Source, UriKind.Absolute, out var source))
                {
                    throw new CopyException($"Can't parse source:  '{options.Source}'", false);
                }
                if (!Uri.TryCreate(options.Destination, UriKind.Absolute, out var destination))
                {
                    throw new CopyException(
                        $"Can't parse destination:  '{options.Destination}'",
                        false);
                }
                var sourceBuilder = new UriBuilder(source);
                var sourcePathParts = sourceBuilder.Path.Split('/');
                var destinationBuilder = new UriBuilder(destination);
                var destinationPathParts = destinationBuilder.Path.Split('/');

                if (sourcePathParts.Length != 3)
                {
                    throw new CopyException(
                        $"Source ('{options.Source}') should be of the form 'https://help.kusto.windows.net/Samples/nyc_taxi'",
                        false);
                }
                if (destinationPathParts.Length != 2)
                {
                    throw new CopyException(
                        $"Destination ('{options.Destination}') should be of the form 'https://mycluster.eastus.kusto.windows.net/mydb'",
                        false);
                }

                var sourceDb = sourcePathParts[1];
                var sourceTable = sourcePathParts[2];
                var destinationDb = sourcePathParts[1];

                sourceBuilder.Path = string.Empty;
                destinationBuilder.Path = string.Empty;

                return new MainJobParameterization
                {
                    SourceClusters = ImmutableList.Create(
                        new SourceClusterParameterization
                        {
                            SourceClusterUri = sourceBuilder.ToString(),
                            Databases = ImmutableList.Create(new SourceDatabaseParameterization
                            {
                                DatabaseName = sourceDb,
                                Tables = ImmutableList.Create(new SourceTableParameterization
                                {
                                    TableName = sourceTable
                                }),
                                Destinations = ImmutableList.Create(new DestinationParameterization
                                {
                                    DestinationClusterUri = destinationBuilder.ToString(),
                                    DatabaseName = destinationDb
                                })
                            })
                        })
                };
            }
            else
            {
                throw new NotImplementedException();
            }
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