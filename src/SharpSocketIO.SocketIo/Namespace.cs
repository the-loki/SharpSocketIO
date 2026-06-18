using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using SharpSocketIO.ComponentEmitter;
using SharpSocketIO.SocketIo.Adapter;
using AdapterBase = SharpSocketIO.SocketIo.Adapter.Adapter;
using SharpSocketIO.SocketIo.Contrib;
using SharpSocketIO.SocketIo.Parser;
using SharpSocketIO.SocketIo.Parser.Commons;

namespace SharpSocketIO.SocketIo;

/// <summary>Port of lib/namespace.ts — a namespace holds sockets + adapter + middleware.</summary>
public sealed class Namespace : Emitter<UnitEvents>, IAdapterNamespace
{
    public Server Server { get; }
    public string Name { get; }
    public AdapterBase Adapter { get; }
    public ConcurrentDictionary<string, Socket> Sockets { get; } = new();

    private readonly List<System.Func<Socket, System.Action<string?>, bool>> _fns = new();

    public Namespace(Server server, string name)
    {
        Server = server;
        Name = name;
        Adapter = Server.AdapterFactory(this);
    }

    /// <summary>Adds a newly-connected socket (called by Client.Connect).</summary>
    public Socket Add(Client client, string? auth)
    {
        var id = Base64Id.GenerateId();
        var handshake = new Handshake { Id = id, Auth = auth };
        var socket = new Socket(this, client, handshake, id);
        RunMiddleware(socket, 0, () =>
        {
            Sockets[id] = socket;
            socket.Join(id); // each socket is in a room named after itself
            socket.OnConnect();
            EmitReserved("connection", socket);
        });
        return socket;
    }

    public void Remove(Socket socket) => Sockets.TryRemove(socket.Id, out _);

    public void Use(System.Func<Socket, System.Action<string?>, bool> middleware) => _fns.Add(middleware);

    private void RunMiddleware(Socket socket, int index, System.Action done)
    {
        if (index >= _fns.Count) { done(); return; }
        _fns[index](socket, err =>
        {
            if (err != null)
            {
                // send connect_error to the client (best-effort)
                socket.Client.SendPacket(new Packet { Type = PacketType.ConnectError, Nsp = Name, Data = err });
                return;
            }
            RunMiddleware(socket, index + 1, done);
        });
    }

    /// <summary>Broadcasts a packet to a set of rooms (used by To/In).</summary>
    public async Task BroadcastAsync(string packet, ISet<string> rooms, ISet<string>? except = null)
    {
        await Adapter.BroadcastAsync(packet, new BroadcastOptions { Rooms = rooms, Except = except });
    }

    /// <summary>IAdapterNamespace: send a packet to a specific socket by id.</summary>
    void IAdapterNamespace.Send(string socketId, string packet)
    {
        if (Sockets.TryGetValue(socketId, out var socket))
        {
            // 5A: the broadcast path passes a pre-encoded string packet; deliver as-is
            socket.Client.SendRaw(packet);
        }
    }
}
