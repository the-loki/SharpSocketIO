using System;
using System.Collections.Generic;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of createPacketEncoderStream (lib/index.ts). Push/pull adapter:
/// each Write(packet) enqueues a header chunk then a payload chunk, in order,
/// exactly as the JS TransformStream would. ReadChunk pulls them one at a time.
/// </summary>
public sealed class PacketEncoderStream
{
    private readonly Queue<byte[]> _chunks = new();

    public void Write(Packet packet)
    {
        EncodePacket.EncodeToBinary(packet, encoded =>
        {
            byte[] bytes = encoded.Kind == RawDataKind.ByteArray
                ? encoded.AsByteArray()!
                : encoded.Kind == RawDataKind.ArrayBuffer
                    ? encoded.AsArrayBuffer()!.Value.ToArray()
                    : throw new InvalidOperationException("EncodeToBinary must yield bytes");

            long payloadLength = bytes.Length;
            byte[] header;
            if (payloadLength < 126)
            {
                header = new byte[1];
                header[0] = (byte)payloadLength;
            }
            else if (payloadLength < 65536)
            {
                header = new byte[3];
                header[0] = 126;
                header[1] = (byte)((payloadLength >> 8) & 0xff);
                header[2] = (byte)(payloadLength & 0xff);
            }
            else
            {
                header = new byte[9];
                header[0] = 127;
                ulong v = (ulong)payloadLength;
                for (int i = 0; i < 8; i++)
                    header[8 - i] = (byte)((v >> (8 * i)) & 0xff);
            }
            // first bit indicates binary (1) vs plain text (0)
            bool isBinary = packet.Data is { Kind: RawDataKind.ByteArray or RawDataKind.ArrayBuffer };
            if (isBinary) header[0] |= 0x80;

            _chunks.Enqueue(header);
            _chunks.Enqueue(bytes);
        });
    }

    public byte[] ReadChunk() => _chunks.Dequeue();

    public bool HasChunks => _chunks.Count > 0;
}
