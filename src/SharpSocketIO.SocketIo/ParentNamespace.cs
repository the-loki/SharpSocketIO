using System.Collections.Generic;
using AdapterType = SharpSocketIO.SocketIo.Adapter.Adapter;
using SharpSocketIO.SocketIo.Adapter;

namespace SharpSocketIO.SocketIo;

/// <summary>
/// Port of lib/parent-namespace.ts. A parent namespace fans out emits to its dynamic
/// child namespaces (created via regex/function). Each child is a regular Namespace.
/// </summary>
public sealed class ParentNamespace
{
    public Server Server { get; }
    public string Name { get; }
    private readonly HashSet<Namespace> _children = new();
    public IReadOnlyCollection<Namespace> Children => _children;

    public ParentNamespace(Server server)
    {
        Server = server;
        Name = "/_parent";
    }

    public void AddChild(Namespace child) => _children.Add(child);

    /// <summary>Fans an event emit out to all child namespaces.</summary>
    public void Emit(string eventName, params object[] args)
    {
        foreach (var child in _children)
        {
            _ = child.EmitAsync(eventName, args);
        }
    }
}
