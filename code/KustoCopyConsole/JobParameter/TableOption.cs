﻿namespace KustoCopyConsole.JobParameter
{
    public class TableOption
    {
        public ExportMode ExportMode { get; set; } = ExportMode.BackfillAndNew;

        public TimeSpan IterationWait { get; set; } = TimeSpan.FromMinutes(5);

        internal void Validate()
        {
        }
    }
}