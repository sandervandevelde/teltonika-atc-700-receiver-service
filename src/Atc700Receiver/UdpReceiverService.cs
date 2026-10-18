using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Atc700Receiver;

public sealed class UdpReceiverService(
    ILogger<UdpReceiverService> logger,
    IConfiguration config,
    KafkaJsonForwarder kafkaForwarder) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private long _messageNumber;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int port = config.GetValue("UDP_PORT", 5027);
        using var udp = new UdpClient(port);
        logger.LogInformation("Listening for UDP on port {Port}", port);

        while (!stoppingToken.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await udp.ReceiveAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (SocketException ex)
            {
                logger.LogWarning(ex, "UDP receive failed");
                continue;
            }

            long messageNumber = Interlocked.Increment(ref _messageNumber);
            DateTime ingestionTimestampUtc = DateTime.UtcNow;
            UdpAvlPacket? packet = null;
            IReadOnlyList<AvlRecord>? records = null;
            try
            {
                packet = ParseDatagram(result.Buffer);
                records = Codec8ExtendedParser.ParseDataField(packet.DataField);
                string jsonMessage = SerializeMessage(
                    messageNumber, ingestionTimestampUtc, result.RemoteEndPoint.ToString() ?? string.Empty,
                    result.Buffer.Length, "success", packet.Imei, records, null, null);

                try
                {
                    await kafkaForwarder.ForwardAsync(packet.Imei, messageNumber, jsonMessage, stoppingToken);
                }
                catch (Exception ex) when (ex is KafkaException or InvalidOperationException)
                {
                    logger.LogError("{JsonMessage}", SerializeForwardFailure(messageNumber, jsonMessage, ex.Message));
                    continue;
                }

                logger.LogInformation("{JsonMessage}", jsonMessage);
                await udp.SendAsync(BuildAcknowledgement(packet, records.Count), result.RemoteEndPoint, stoppingToken);
            }
            catch (Exception ex) when (ex is FormatException or NotSupportedException or ArgumentOutOfRangeException or OverflowException)
            {
                logger.LogWarning("{JsonMessage}", SerializeMessage(
                    messageNumber, ingestionTimestampUtc, result.RemoteEndPoint.ToString() ?? string.Empty,
                    result.Buffer.Length, "error", packet?.Imei, records, ex.Message,
                    Convert.ToHexString(result.Buffer)));
            }
        }
    }

    private static string SerializeMessage(
        long messageNumber,
        DateTime ingestionTimestampUtc,
        string sender,
        int byteCount,
        string decodeStatus,
        string? imei,
        IReadOnlyList<AvlRecord>? records,
        string? decodeError,
        string? rawPayloadHex)
    {
        object[] recordItems = records is null
            ? Array.Empty<object>()
            : records.Select((record, index) => (object)new
            {
                recordNumber = index + 1,
                timestampUtc = record.Timestamp,
                record.Priority,
                latitude = record.Latitude,
                longitude = record.Longitude,
                record.Altitude,
                record.Angle,
                record.Satellites,
                record.Speed,
                record.EventIoId,
                io = record.Io.Select(DecodeIoElement)
            }).ToArray();

        var message = new
        {
            messageNumber,
            ingestionTimestampUtc,
            sender,
            byteCount,
            decodeStatus,
            decodeError,
            rawPayloadHex,
            imei,
            recordCount = records?.Count,
            alarmButton = records is null ? "unknown" : GetAlarmButtonState(records),
            records = recordItems
        };

        return JsonSerializer.Serialize(message, JsonOptions);
    }

    private static string SerializeForwardFailure(long messageNumber, string jsonMessage, string error)
    {
        using JsonDocument originalMessage = JsonDocument.Parse(jsonMessage);
        return JsonSerializer.Serialize(new
        {
            messageNumber,
            forwardingStatus = "error",
            forwardingError = error,
            message = originalMessage.RootElement
        }, JsonOptions);
    }

    private static JsonIoElement DecodeIoElement(IoElement element)
    {
        string rawHex = Convert.ToHexString(element.Value);
        ulong? unsignedValue = ReadUnsignedValue(element.Value);

        return element.Id switch
        {
            21 when element.Value.Length == 1 && unsignedValue.HasValue =>
                new(element.Id, "gsmSignal", rawHex, unsignedValue.Value, "level"),
            67 when element.Value.Length == 2 && unsignedValue.HasValue =>
                new(element.Id, "batteryVoltage", rawHex, unsignedValue.Value, "mV"),
            68 when element.Value.Length == 2 && unsignedValue.HasValue =>
                new(element.Id, "batteryCurrent", rawHex, unsignedValue.Value, "mA"),
            69 when element.Value.Length == 1 && unsignedValue.HasValue =>
                new(element.Id, "gnssStatus", rawHex, GetGnssStatus((byte)unsignedValue.Value), null),
            70 when element.Value.Length == 2 =>
                new(element.Id, "pcbTemperature", rawHex,
                    BinaryPrimitives.ReadInt16BigEndian(element.Value) / 10.0, "°C"),
            113 when element.Value.Length == 1 && unsignedValue.HasValue =>
                new(element.Id, "batteryLevel", rawHex, unsignedValue.Value, "%"),
            16 when element.Value.Length == 4 && unsignedValue.HasValue =>
                new(element.Id, "totalOdometer", rawHex, unsignedValue.Value, "m"),
            181 when element.Value.Length == 2 && unsignedValue.HasValue =>
                new(element.Id, "gnssPdop", rawHex, unsignedValue.Value / 10.0, "m"),
            182 when element.Value.Length == 2 && unsignedValue.HasValue =>
                new(element.Id, "gnssHdop", rawHex, unsignedValue.Value / 10.0, "m"),
            236 when element.Value.Length == 1 && unsignedValue.HasValue =>
                new(element.Id, "alarm", rawHex, unsignedValue.Value == 1, null),
            240 when element.Value.Length == 1 && unsignedValue.HasValue =>
                new(element.Id, "movement", rawHex, unsignedValue.Value == 1 ? "moving" : "onStop", null),
            _ => new(element.Id, "unknown", rawHex, null, null)
        };
    }

    private static ulong? ReadUnsignedValue(byte[] bytes) => bytes.Length switch
    {
        1 => bytes[0],
        2 => BinaryPrimitives.ReadUInt16BigEndian(bytes),
        4 => BinaryPrimitives.ReadUInt32BigEndian(bytes),
        8 => BinaryPrimitives.ReadUInt64BigEndian(bytes),
        _ => null
    };

    private static string GetGnssStatus(byte value) => value switch
    {
        0 => "off",
        1 => "onWithFix",
        2 => "onWithoutFix",
        3 => "sleeping",
        _ => "unknown"
    };

    private sealed record JsonIoElement(ushort Id, string Name, string ValueHex, object? Value, string? Unit);

    private static string GetAlarmButtonState(IReadOnlyList<AvlRecord> records)
    {
        bool alarmReported = false;
        foreach (AvlRecord record in records)
        {
            if (record.EventIoId == 236)
                alarmReported = true;

            foreach (IoElement element in record.Io)
            {
                if (element.Id != 236)
                    continue;

                alarmReported = true;
                if (element.Value.Length == 1 && element.Value[0] == 1)
                    return "pressed";
            }
        }

        return alarmReported ? "not pressed" : "not reported";
    }

    private static UdpAvlPacket ParseDatagram(ReadOnlySpan<byte> datagram)
    {
        if (datagram.Length < 8)
            throw new FormatException("UDP datagram is shorter than the Teltonika header.");

        int declaredLength = BinaryPrimitives.ReadUInt16BigEndian(datagram);
        if (declaredLength != datagram.Length - 2)
            throw new FormatException($"Length mismatch: header {declaredLength}, actual {datagram.Length - 2}.");

        ushort packetId = BinaryPrimitives.ReadUInt16BigEndian(datagram[2..]);
        if (datagram[4] != 1)
            throw new FormatException($"Invalid not-usable byte 0x{datagram[4]:X2}.");

        byte avlPacketId = datagram[5];
        int imeiLength = BinaryPrimitives.ReadUInt16BigEndian(datagram[6..]);
        if (imeiLength != 15 || datagram.Length < 8 + imeiLength)
            throw new FormatException($"Invalid IMEI length {imeiLength}.");

        ReadOnlySpan<byte> imeiBytes = datagram.Slice(8, imeiLength);
        string imei = Encoding.ASCII.GetString(imeiBytes);
        if (imei.Any(character => character is < '0' or > '9'))
            throw new FormatException("IMEI contains non-digit characters.");

        return new UdpAvlPacket(packetId, avlPacketId, imei, datagram[(8 + imeiLength)..].ToArray());
    }

    private static byte[] BuildAcknowledgement(UdpAvlPacket packet, int acceptedCount)
    {
        var acknowledgement = new byte[7];
        BinaryPrimitives.WriteUInt16BigEndian(acknowledgement, 5);
        BinaryPrimitives.WriteUInt16BigEndian(acknowledgement.AsSpan(2), packet.PacketId);
        acknowledgement[4] = 1;
        acknowledgement[5] = packet.AvlPacketId;
        acknowledgement[6] = checked((byte)acceptedCount);
        return acknowledgement;
    }

    private sealed record UdpAvlPacket(ushort PacketId, byte AvlPacketId, string Imei, byte[] DataField);
}