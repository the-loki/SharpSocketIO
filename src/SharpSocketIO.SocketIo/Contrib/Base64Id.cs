using System;
using System.Security.Cryptography;

namespace SharpSocketIO.SocketIo.Contrib;

internal static class Base64Id
{
    private static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create();

    public static string GenerateId()
    {
        var bytes = new byte[15];
        Rng.GetBytes(bytes);
        return Convert.ToBase64String(bytes).Replace('/', '_').Replace('+', '-');
    }
}
