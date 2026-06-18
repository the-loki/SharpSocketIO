using System.Collections;
using System.Collections.Generic;

namespace SharpSocketIO.SocketIo.Parser;

/// <summary>
/// Port of lib/is-binary.ts. In .NET, binary values are byte[] or ArraySegment&lt;byte&gt;
/// (Blob/File have no .NET primitive — see design spec §6.1).
/// </summary>
public static class IsBinaryHelper
{
    public static bool IsBinary(object? obj)
    {
        if (obj is null) return false;
        return obj is byte[] or ArraySegment<byte>;
    }

    /// <summary>
    /// Deep-scan for any binary leaf. Mirrors JS hasBinary: arrays/objects recurse,
    /// a ToJSON() method (parameterless) is invoked and its result scanned.
    /// Uses reference tracking to guard against cycles (circular inputs are expected
    /// to be rejected downstream; HasBinary itself must not StackOverflow on them).
    /// </summary>
    public static bool HasBinary(object? obj)
        => HasBinary(obj, new HashSet<object>(new RefEqComparer()));

    private static bool HasBinary(object? obj, HashSet<object> visited)
    {
        if (obj is null) return false;
        if (IsBinary(obj)) return true;
        if (obj is string) return false; // strings are enumerable but not binary carriers

        // Cycle guard: if we've already visited this reference, don't descend again.
        if (!visited.Add(obj)) return false;

        try
        {
            if (obj is IDictionary dict)
            {
                foreach (DictionaryEntry kv in dict)
                    if (HasBinary(kv.Value, visited)) return true;
                return false;
            }
            if (obj is IList list)
            {
                foreach (var item in list)
                    if (HasBinary(item, visited)) return true;
                return false;
            }
            if (obj.GetType().IsPrimitive || obj is ValueType) return false;

            if (obj.GetType().GetMethod("ToJSON", Type.EmptyTypes) is { } toJSON)
            {
                var json = toJSON.Invoke(obj, null);
                return HasBinary(json, visited);
            }
            foreach (var prop in obj.GetType().GetProperties())
            {
                object? val;
                try { val = prop.GetValue(obj); }
                catch { continue; }
                if (HasBinary(val, visited)) return true;
            }
            return false;
        }
        finally
        {
            // remove so sibling-equal references elsewhere can still be scanned;
            // matches DFS semantics without leaking memory across unrelated calls.
            visited.Remove(obj);
        }
    }

    // Reference equality comparer (works on netstandard2.1 where ReferenceEqualityComparer is absent).
    private sealed class RefEqComparer : IEqualityComparer<object>
    {
        public new bool Equals(object? x, object? y) => ReferenceEquals(x, y);
        public int GetHashCode(object obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }
}
