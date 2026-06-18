using System.Text.Json;
using System.Text.Json.Serialization;

namespace SharpSocketIO.SocketIo.Parser.Commons;

/// <summary>
/// Shared JSON options: preserve exact object semantics like JS JSON.stringify/parse.
/// JS writes integers without decimal point; System.Text.Json does this for int/long
/// automatically. Dictionaries serialize as JSON objects preserving key insertion order.
/// </summary>
internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        IncludeFields = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };
}
