using System;
using System.Collections.Generic;
using System.Text;
using SharpSocketIO.EngineIo.Parser.Commons;

namespace SharpSocketIO.EngineIo.Parser;

/// <summary>
/// Port of createPacketDecoderStream (lib/index.ts). Push/pull adapter with the
/// same READ_HEADER / READ_EXTENDED_LENGTH_16 / READ_EXTENDED_LENGTH_64 /
/// READ_PAYLOAD state machine and the same guards (len==0 → error,
/// len &gt; maxPayload → error, 64-bit high-word &gt; 2^(53-32)-1 → error).
/// </summary>
public sealed class PacketDecoderStream
{
    private enum State { ReadHeader, ReadExtendedLength16, ReadExtendedLength64, ReadPayload }

    private readonly long _maxPayload;
    private readonly BinaryType _binaryType;
    private readonly List<byte[]> _chunks = new();
    private readonly Queue<Packet> _output = new();
    private State _state = State.ReadHeader;
    private long _expectedLength = -1;
    private bool _isBinary;

    public PacketDecoderStream(long maxPayload, BinaryType binaryType)
    {
        _maxPayload = maxPayload;
        _binaryType = binaryType;
    }

    public void Write(byte[] chunk)
    {
        _chunks.Add(chunk);
        while (true)
        {
            if (_state == State.ReadHeader)
            {
                if (TotalLength() < 1) break;
                var header = ConcatChunks(1);
                _isBinary = (header[0] & 0x80) == 0x80;
                _expectedLength = header[0] & 0x7f;
                if (_expectedLength < 126) _state = State.ReadPayload;
                else if (_expectedLength == 126) _state = State.ReadExtendedLength16;
                else _state = State.ReadExtendedLength64;
            }
            else if (_state == State.ReadExtendedLength16)
            {
                if (TotalLength() < 2) break;
                var h = ConcatChunks(2);
                _expectedLength = ((long)h[0] << 8) | h[1];
                _state = State.ReadPayload;
            }
            else if (_state == State.ReadExtendedLength64)
            {
                if (TotalLength() < 8) break;
                var h = ConcatChunks(8);
                // high 32 bits (network order)
                long n = ((long)h[0] << 24) | ((long)h[1] << 16) | ((long)h[2] << 8) | h[3];
                if (n > (long)Math.Pow(2, 53 - 32) - 1)
                {
                    _output.Enqueue(ErrorPacket.Instance);
                    break;
                }
                long low = ((long)h[4] << 24) | ((long)h[5] << 16) | ((long)h[6] << 8) | h[7];
                _expectedLength = n * (long)Math.Pow(2, 32) + low;
                _state = State.ReadPayload;
            }
            else // ReadPayload
            {
                if (TotalLength() < _expectedLength) break;
                var data = ConcatChunks((int)_expectedLength);
                RawData encoded = _isBinary
                    ? new RawData(data)
                    : new RawData(Encoding.UTF8.GetString(data));
                _output.Enqueue(DecodePacket.Decode(encoded, _binaryType));
                _state = State.ReadHeader;
            }

            if (_expectedLength == 0 || _expectedLength > _maxPayload)
            {
                _output.Enqueue(ErrorPacket.Instance);
                break;
            }
        }
    }

    public bool TryRead(out Packet packet) => _output.TryDequeue(out packet!);

    private int TotalLength()
    {
        int total = 0;
        for (int i = 0; i < _chunks.Count; i++) total += _chunks[i].Length;
        return total;
    }

    // Mirrors JS concatChunks: pulls `size` bytes from the head of _chunks,
    // slicing the head chunk if it contributes more than remains.
    private byte[] ConcatChunks(int size)
    {
        var buffer = new byte[size];
        int j = 0;
        for (int i = 0; i < size; i++)
        {
            buffer[i] = _chunks[0][j++];
            if (j == _chunks[0].Length)
            {
                _chunks.RemoveAt(0);
                j = 0;
            }
        }
        if (_chunks.Count > 0 && j < _chunks[0].Length)
        {
            var remaining = new byte[_chunks[0].Length - j];
            Array.Copy(_chunks[0], j, remaining, 0, remaining.Length);
            _chunks[0] = remaining;
        }
        return buffer;
    }
}
