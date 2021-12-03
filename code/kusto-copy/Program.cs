using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

// See https://aka.ms/new-console-template for more information

namespace kusto_copy
{
    internal class Program
    {
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
            Console.WriteLine();

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
            //catch (DeltaException ex)
            //{
            //    DisplayDeltaException(ex);

            //    return 1;
            //}
            catch (Exception ex)
            {
                //DisplayGenericException(ex);

                return 1;
            }
        }

        private static async Task RunOptionsAsync(CommandLineOptions options)
        {
            if (options.Verbose)
            {
                Console.WriteLine("Verbose output enabled");
            }

            //  Dependency injection
            //var tracer = new ConsoleTracer(options.Verbose);
            //var apiClient = new ApiClient(tracer, new SimpleHttpClientFactory(tracer));
            //var orchestration = new DeltaOrchestration(
            //    tracer,
            //    apiClient);
            //var success = await orchestration.ComputeDeltaAsync(
            //    options.ParameterFilePath,
            //    options.Overrides);

            //if (!success)
            //{
            //    throw new DeltaException("Failure due to drop commands");
            //}

            await Task.CompletedTask;
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