using System.Buffers.Binary;

namespace Atc700Receiver;

public sealed record IoElement(ushort Id, byte[] Value);

public sealed record AvlRecord(
    DateTimeOffset Timestamp, byte Priority,
    double Longitude, double Latitude, short Altitude, ushort Angle, byte Satellites, ushort Speed,
    ushort EventIoId, IReadOnlyList<IoElement> Io);

public static class Codec8ExtendedParser
{
    private const byte CodecId = 0x8E;

    public static IReadOnlyList<AvlRecord> ParseDataField(ReadOnlySpan<byte> data)
    {
        var reader = new Reader(data);
        byte codec = reader.U8();
        if (codec != CodecId)
            throw new NotSupportedException($"Unsupported codec 0x{codec:X2}; expected 0x8E.");

        int count = reader.U8();
        var records = new List<AvlRecord>(count);
        for (int i = 0; i < count; i++)
            records.Add(ReadRecord(ref reader));

        int countEnd = reader.U8();
        if (countEnd != count)
            throw new FormatException($"Record count mismatch: {count} vs {countEnd}.");
        if (reader.Remaining != 0)
            throw new FormatException($"Unexpected {reader.Remaining} trailing bytes in Codec 8 Extended data field.");

        return records;
    }

    public static ushort ComputeCrc16(ReadOnlySpan<byte> data)
    {
        ushort crc = 0;
        foreach (byte value in data)
        {
            crc ^= value;
            for (int bit = 0; bit < 8; bit++)
                crc = (ushort)((crc & 1) != 0 ? (crc >> 1) ^ 0xA001 : crc >> 1);
        }
        return crc;
    }

    private static AvlRecord ReadRecord(ref Reader reader)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds((long)reader.U64());
        byte priority = reader.U8();
        double longitude = reader.I32() / 10_000_000.0;
        double latitude = reader.I32() / 10_000_000.0;
        short altitude = reader.I16();
        ushort angle = reader.U16();
        byte satellites = reader.U8();
        ushort speed = reader.U16();

        ushort eventIoId = reader.U16();
        int totalIoCount = reader.U16();
        var io = new List<IoElement>(totalIoCount);
        int parsedIoCount = 0;
        foreach (int size in new[] { 1, 2, 4, 8 })
        {
            int count = reader.U16();
            for (int i = 0; i < count; i++)
            {
                ushort id = reader.U16();
                io.Add(new IoElement(id, reader.Bytes(size)));
                parsedIoCount++;
            }
        }

        int variableIoCount = reader.U16();
        for (int i = 0; i < variableIoCount; i++)
        {
            ushort id = reader.U16();
            int length = reader.U16();
            io.Add(new IoElement(id, reader.Bytes(length)));
            parsedIoCount++;
        }

        if (parsedIoCount != totalIoCount)
            throw new FormatException($"IO count mismatch: declared {totalIoCount}, parsed {parsedIoCount}.");

        return new AvlRecord(timestamp, priority, longitude, latitude, altitude, angle, satellites, speed, eventIoId, io);
    }

    private ref struct Reader
    {
        private readonly ReadOnlySpan<byte> _data;
        private int _position;

        public Reader(ReadOnlySpan<byte> data)
        {
            _data = data;
            _position = 0;
        }

        public int Remaining => _data.Length - _position;

        private ReadOnlySpan<byte> Take(int length)
        {
            if (length < 0 || _position + length > _data.Length)
                throw new FormatException("Unexpected end of Codec 8 Extended data field.");
            ReadOnlySpan<byte> result = _data.Slice(_position, length);
            _position += length;
            return result;
        }

        public byte U8() => Take(1)[0];
        public ushort U16() => BinaryPrimitives.ReadUInt16BigEndian(Take(2));
        public short I16() => BinaryPrimitives.ReadInt16BigEndian(Take(2));
        public int I32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));
        public ulong U64() => BinaryPrimitives.ReadUInt64BigEndian(Take(8));
        public byte[] Bytes(int length) => Take(length).ToArray();
    }
}