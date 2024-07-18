﻿namespace KustoCopyConsole.Storage.Entity.State
{
    public enum SourceBlockState
    {
        Pending,
        Exporting,
        Exported,
        /// <summary>
        /// Between <see cref="Exported"/> and <see cref="Completed"/>,
        /// <see cref="SourceUrlEntity"/> are written.  If <see cref="Completed"/>
        /// isn't present, it means all URLs were not written.
        /// </summary>
        Completed
    }
}