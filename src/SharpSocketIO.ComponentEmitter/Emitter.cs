using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace SharpSocketIO.ComponentEmitter;

/// <summary>
/// C# port of @socket.io/component-emitter. Provides string-keyed event dispatch
/// with On/Once/Off/Emit/Listeners/HasListeners. Generic over <typeparamref name="TEvents"/>
/// so subclasses can declare a marker event-map type (mirrors upstream
/// Emitter&lt;ListenEvents, EmitEvents, ReservedEvents&gt;), but dispatch itself is
/// string-keyed to match the untyped JS API and the ported test suite.
/// </summary>
public class Emitter<TEvents>
{
    private Dictionary<string, List<Delegate>>? _callbacks;

    protected Emitter() { }

    public Emitter<TEvents> On(string eventName, Delegate fn) => AddListener(eventName, fn);

    public Emitter<TEvents> On(string eventName, Action fn) => AddListener(eventName, fn);

    public Emitter<TEvents> On(string eventName, Action<object[]> fn) => AddListener(eventName, fn);

    private Emitter<TEvents> AddListener(string eventName, Delegate fn)
    {
        _callbacks ??= new Dictionary<string, List<Delegate>>();
        if (!_callbacks.TryGetValue(eventName, out var list))
        {
            list = new List<Delegate>();
            _callbacks[eventName] = list;
        }
        list.Add(fn);
        return this;
    }

    /// <summary>Adds a listener that fires once then auto-removes.</summary>
    public Emitter<TEvents> Once(string eventName, Action<object[]> fn)
    {
        void Wrapper(object[] args)
        {
            Off(eventName, (Action<object[]>)Wrapper);
            fn(args);
        }
        WrapperMap.Set((Action<object[]>)Wrapper, fn);
        return On(eventName, (Action<object[]>)Wrapper);
    }

    /// <summary>Adds a parameterless listener that fires once then auto-removes.</summary>
    public Emitter<TEvents> Once(string eventName, Action fn)
    {
        Action<object[]> wrapper = null!;
        wrapper = _ =>
        {
            Off(eventName, wrapper);
            fn();
        };
        WrapperMap.Set(wrapper, fn);
        return On(eventName, wrapper);
    }

    public Emitter<TEvents> Off(string eventName, Delegate fn) => RemoveListener(eventName, fn);

    public Emitter<TEvents> Off(string eventName)
    {
        _callbacks?.Remove(eventName);
        return this;
    }

    public Emitter<TEvents> Off()
    {
        _callbacks?.Clear();
        return this;
    }

    private Emitter<TEvents> RemoveListener(string eventName, Delegate fn)
    {
        if (_callbacks == null) return this;
        if (!_callbacks.TryGetValue(eventName, out var list)) return this;
        for (int i = 0; i < list.Count; i++)
        {
            var cb = list[i];
            if (cb == fn || WrapperMap.Get(cb) == fn)
            {
                list.RemoveAt(i);
                break;
            }
        }
        if (list.Count == 0) _callbacks.Remove(eventName);
        return this;
    }

    /// <summary>Emit an event with the given args. Handlers may mutate the listener set during emit (snapshot taken).</summary>
    public Emitter<TEvents> Emit(string eventName, params object[] args)
    {
        if (_callbacks == null) return this;
        if (!_callbacks.TryGetValue(eventName, out var list)) return this;
        var snapshot = list.ToArray();
        foreach (var cb in snapshot)
        {
            InvokeHandler(cb, args);
        }
        return this;
    }

    /// <summary>Alias of Emit, used for reserved events (protected in JS).</summary>
    public Emitter<TEvents> EmitReserved(string eventName, params object[] args) => Emit(eventName, args);

    public IReadOnlyList<Delegate> Listeners(string eventName)
    {
        if (_callbacks != null && _callbacks.TryGetValue(eventName, out var list))
            return list;
        return Array.Empty<Delegate>();
    }

    public bool HasListeners(string eventName) => Listeners(eventName).Count > 0;

    private static void InvokeHandler(Delegate cb, object[] args)
    {
        switch (cb)
        {
            case Action action:
                action();
                break;
            case Action<object[]> arr:
                arr(args);
                break;
            default:
                cb.DynamicInvoke(args);
                break;
        }
    }

    // Maps a Once wrapper back to its original fn so Off(eventName, originalFn) finds it.
    private static class WrapperMap
    {
        private static readonly ConcurrentDictionary<Delegate, Delegate> s_map = new();

        public static void Set(Delegate wrapper, Delegate original) => s_map[wrapper] = original;

        public static Delegate? Get(Delegate wrapper) => s_map.TryGetValue(wrapper, out var o) ? o : null;
    }
}

/// <summary>Marker base for typed event maps.</summary>
public abstract class EmitterEvents { }

/// <summary>Marker type for untyped emitters.</summary>
public sealed class UnitEvents : EmitterEvents { }

/// <summary>Non-generic Emitter convenience (events type = UnitEvents marker).</summary>
public class Emitter : Emitter<UnitEvents>
{
    public Emitter() { }
}
