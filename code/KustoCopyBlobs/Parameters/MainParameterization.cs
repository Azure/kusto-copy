﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoCopyBlobs.Parameters
{
    public class MainParameterization
    {
        public SourceParameterization? Source { get; set; }
        
        public DestinationParameterization[]? Destinations { get; set; }
    }
}