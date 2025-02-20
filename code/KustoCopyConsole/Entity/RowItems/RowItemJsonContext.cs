using KustoCopyConsole.Entity.State;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace KustoCopyConsole.Entity.RowItems
{
    [JsonSourceGenerationOptions(
        WriteIndented = false,
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
        Converters = [
            typeof(JsonStringEnumConverter<ActivityState>),
            typeof(JsonStringEnumConverter<BlockState>),
            typeof(JsonStringEnumConverter<IterationState>),
            typeof(JsonStringEnumConverter<TempTableState>),
            typeof(JsonStringEnumConverter<UrlState>)
        ],
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    [JsonSerializable(typeof(RowItemBase))]
    [JsonSerializable(typeof(ActivityRowItem))]
    [JsonSerializable(typeof(BlockRowItem))]
    [JsonSerializable(typeof(IterationRowItem))]
    [JsonSerializable(typeof(TempTableRowItem))]
    [JsonSerializable(typeof(UrlRowItem))]
    [JsonSerializable(typeof(ExtentRowItem))]
    internal partial class RowItemJsonContext : JsonSerializerContext
    {
    }
}