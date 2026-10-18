using Confluent.Kafka;

namespace Atc700Receiver;

public sealed class KafkaJsonForwarder : IDisposable
{
    private readonly ILogger<KafkaJsonForwarder> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private int _connectionState;
    private int _hasConnected;

    public KafkaJsonForwarder(IConfiguration configuration, ILogger<KafkaJsonForwarder> logger)
    {
        _logger = logger;
        _topic = GetRequired(configuration, "KAFKA_TOPIC");

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = GetRequired(configuration, "KAFKA_BOOTSTRAP_SERVERS"),
            SecurityProtocol = ParseEnum<SecurityProtocol>(configuration, "KAFKA_SECURITY_PROTOCOL"),
            SaslMechanism = ParseEnum<SaslMechanism>(configuration, "KAFKA_SASL_MECHANISM"),
            SaslUsername = GetRequired(configuration, "KAFKA_SASL_USERNAME"),
            SaslPassword = GetRequired(configuration, "KAFKA_SASL_PASSWORD"),
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = 60000,
            RequestTimeoutMs = 60000,
            ConnectionsMaxIdleMs = 180000,
            MetadataMaxAgeMs = 180000,
            SocketKeepaliveEnable = true
        };

        _producer = new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler(HandleKafkaError)
            .Build();

        _logger.LogInformation(
            "Kafka producer configured for {BootstrapServers}, topic {Topic}, using SASL_SSL/PLAIN",
            producerConfig.BootstrapServers, _topic);
    }

    public async Task ForwardAsync(string imei, long messageNumber, string json, CancellationToken cancellationToken)
    {
        try
        {
            DeliveryResult<string, string> delivery = await _producer.ProduceAsync(
                _topic,
                new Message<string, string> { Key = imei, Value = json },
                cancellationToken);

            if (delivery.Status != PersistenceStatus.Persisted)
                throw new InvalidOperationException($"Kafka did not confirm persistence (status {delivery.Status}).");

            int previousState = Interlocked.Exchange(ref _connectionState, 1);
            if (Interlocked.Exchange(ref _hasConnected, 1) == 0)
            {
                _logger.LogInformation(
                    "Kafka connection established; broker confirmed message {MessageNumber} on topic {Topic}",
                    messageNumber, delivery.Topic);
            }
            else if (previousState == 2)
            {
                _logger.LogInformation(
                    "Kafka connection re-established; broker confirmed message {MessageNumber} on topic {Topic}",
                    messageNumber, delivery.Topic);
            }
        }
        catch (Exception ex) when (ex is KafkaException or InvalidOperationException)
        {
            int previousState = Interlocked.Exchange(ref _connectionState, 2);
            if (previousState != 2)
            {
                _logger.LogError(
                    ex,
                    "Kafka delivery failed for message {MessageNumber}; the tracker datagram will not be acknowledged",
                    messageNumber);
            }
            throw;
        }
    }

    private void HandleKafkaError(IProducer<string, string> _, Error error)
    {
        if (error.IsFatal || error.Code == ErrorCode.Local_AllBrokersDown)
        {
            int previousState = Interlocked.Exchange(ref _connectionState, 2);
            if (previousState != 2)
                _logger.LogError("Kafka connection failed: {ErrorCode} {Reason}", error.Code, error.Reason);
        }
        else
        {
            _logger.LogWarning("Kafka client error: {ErrorCode} {Reason}", error.Code, error.Reason);
        }
    }

    private static string GetRequired(IConfiguration configuration, string name) =>
        !string.IsNullOrWhiteSpace(configuration[name])
            ? configuration[name]!
            : throw new InvalidOperationException($"Required Kafka setting {name} is missing.");

    private static TEnum ParseEnum<TEnum>(IConfiguration configuration, string name)
        where TEnum : struct, Enum
    {
        string value = GetRequired(configuration, name).Replace("_", string.Empty, StringComparison.Ordinal);
        return Enum.TryParse(value, true, out TEnum parsed)
            ? parsed
            : throw new InvalidOperationException($"Kafka setting {name} has unsupported value '{value}'.");
    }

    public void Dispose() => _producer.Dispose();
}