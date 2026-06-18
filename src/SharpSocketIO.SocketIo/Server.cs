using System.Collections.Concurrent;
using System.Collections.Generic;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Adapter;
using AdapterBase = SharpSocketIO.SocketIo.Adapter.Adapter;
using SharpSocketIO.SocketIo.Parser;

namespace SharpSocketIO.SocketIo;

/// <summary>Port of lib/index.ts — the socket.io server.</summary>
public sealed class Server : Emitter<UnitEvents>
{
    public ServerOptions Options { get; } = new();
    public Encoder Encoder { get; } = new();
    public ConcurrentDictionary<string, Namespace> Namespaces { get; } = new();
    public System.Func<Namespace, AdapterBase> AdapterFactory { get; set; }

    private readonly ConcurrentDictionary<string, Client> _clients = new();

    public Server()
    {
        AdapterFactory = nsp => new AdapterBase(nsp);
        Of("/");
    }

    /// <summary>Gets or creates a namespace by name.</summary>
    public Namespace Of(string name)
    {
        if (!name.StartsWith("/")) name = "/" + name;
        return Namespaces.GetOrAdd(name, n => new Namespace(this, n));
    }

    /// <summary>Called by 5B when an engine.io connection is established.</summary>
    public Client CreateClient(IEngineIoConnection conn)
    {
        var client = new Client(this, conn);
        _clients[conn.Id] = client;
        conn.Events.On("close", _ => _clients.TryRemove(conn.Id, out Client? _));
        return client;
    }
}
