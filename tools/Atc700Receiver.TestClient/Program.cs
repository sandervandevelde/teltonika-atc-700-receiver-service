using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

const ushort packetId = 0x1234;
const byte avlPacketId = 0x01;
const string imei = "359710000000001";
string host = args.Length > 0 ? args[0] : "127.0.0.1";
int port = args.Length > 1 && int.TryParse(args[1], out int parsedPort) ? parsedPort : 5027;

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
using var client = new UdpClient();

try
{
    byte[] datagram = BuildDatagram();
    Console.WriteLine($"Sending a two-record UDP Codec 8 Extended batch for IMEI {imei} to {host}:{port}...");
    await client.SendAsync(datagram, host, port, timeout.Token);
    UdpReceiveResult response = await client.ReceiveAsync(timeout.Token);
    if (!IsExpectedAcknowledgement(response.Buffer))
        throw new InvalidOperationException($"Unexpected acknowledgement ({Convert.ToHexString(response.Buffer)}).");

    Console.WriteLine($"Receiver acknowledged two records from {response.RemoteEndPoint}.");
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine($"Timed out waiting for a UDP acknowledgement from {host}:{port}.");
    Environment.ExitCode = 1;
}
catch (Exception ex) when (ex is SocketException or InvalidOperationException)
{
    Console.Error.WriteLine(ex.Message);
    Environment.ExitCode = 1;
}

static byte[] BuildDatagram()
{
    using var dataField = new MemoryStream();
    dataField.WriteByte(0x8E);
    dataField.WriteByte(2);
    WriteRecord(dataField, DateTimeOffset.UtcNow, 23_522_000, 488_566_000, 236, true);
    WriteRecord(dataField, DateTimeOffset.UtcNow.AddSeconds(1), 23_522_100, 488_566_100, 0, false);
    dataField.WriteByte(2);

    byte[] avlData = dataField.ToArray();
    byte[] imeiBytes = Encoding.ASCII.GetBytes(imei);
    var payload = new byte[6 + imeiBytes.Length + avlData.Length];
    BinaryPrimitives.WriteUInt16BigEndian(payload, packetId);
    payload[2] = 1;
    payload[3] = avlPacketId;
    BinaryPrimitives.WriteUInt16BigEndian(payload.AsSpan(4), (ushort)imeiBytes.Length);
    imeiBytes.CopyTo(payload, 6);
    avlData.CopyTo(payload, 6 + imeiBytes.Length);

    var datagram = new byte[2 + payload.Length];
    BinaryPrimitives.WriteUInt16BigEndian(datagram, (ushort)payload.Length);
    payload.CopyTo(datagram, 2);
    return datagram;
}

static void WriteRecord(
    Stream stream,
    DateTimeOffset timestamp,
    int longitude,
    int latitude,
    ushort eventIoId,
    bool alarmPressed)
{
    WriteUInt64(stream, (ulong)timestamp.ToUnixTimeMilliseconds());
    stream.WriteByte(0);
    WriteInt32(stream, longitude);
    WriteInt32(stream, latitude);
    WriteInt16(stream, 35);
    WriteUInt16(stream, 90);
    stream.WriteByte(8);
    WriteUInt16(stream, 0);
    WriteUInt16(stream, eventIoId);
    WriteUInt16(stream, (ushort)(alarmPressed ? 1 : 0));

    WriteUInt16(stream, (ushort)(alarmPressed ? 1 : 0));
    if (alarmPressed)
    {
        WriteUInt16(stream, 236);
        stream.WriteByte(1);
    }

    WriteUInt16(stream, 0);
    WriteUInt16(stream, 0);
    WriteUInt16(stream, 0);
    WriteUInt16(stream, 0);
}

static bool IsExpectedAcknowledgement(byte[] acknowledgement) =>
    acknowledgement.Length == 7 &&
    BinaryPrimitives.ReadUInt16BigEndian(acknowledgement) == 5 &&
    BinaryPrimitives.ReadUInt16BigEndian(acknowledgement.AsSpan(2)) == packetId &&
    acknowledgement[4] == 1 &&
    acknowledgement[5] == avlPacketId &&
    acknowledgement[6] == 2;

static void WriteUInt16(Stream stream, ushort value)
{
    Span<byte> bytes = stackalloc byte[2];
    BinaryPrimitives.WriteUInt16BigEndian(bytes, value);
    stream.Write(bytes);
}

static void WriteInt16(Stream stream, short value)
{
    Span<byte> bytes = stackalloc byte[2];
    BinaryPrimitives.WriteInt16BigEndian(bytes, value);
    stream.Write(bytes);
}

static void WriteUInt64(Stream stream, ulong value)
{
    Span<byte> bytes = stackalloc byte[8];
    BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
    stream.Write(bytes);
}

static void WriteInt32(Stream stream, int value)
{
    Span<byte> bytes = stackalloc byte[4];
    BinaryPrimitives.WriteInt32BigEndian(bytes, value);
    stream.Write(bytes);
}
