namespace Kafka.ParallelFlow;

public sealed class RecordConsumerConfig
{
    #region `Kafka.ParallelFlow` config values

    public int AutoCommitIntervalMs { get; init; } = 5_000;

    public int MaxDegreeOfParallelism { get; init; } = 1;

    public int MaxUncommittedMessagesPerMemoryPartition { get; init; } = 10_000;

    #endregion

    #region `Confluent.Kafka` config values

    public string? GroupId { get; init; }

    public string? BootstrapServers { get; init; }

    public bool? SocketNagleDisable { get; init; }

    public AutoOffsetReset? AutoOffsetReset { get; init; }

    public int? SessionTimeoutMs { get; init; }

    public int? HeartbeatIntervalMs { get; init; }

    public int? CoordinatorQueryIntervalMs { get; init; }

    public int? MaxPollIntervalMs { get; init; }

    public int? QueuedMinMessages { get; init; }

    public int? QueuedMaxMessagesKbytes { get; init; }

    public int? FetchWaitMaxMs { get; init; }

    public int? MaxPartitionFetchBytes { get; init; }

    public int? FetchMaxBytes { get; init; }

    public int? FetchMinBytes { get; init; }

    public int? FetchErrorBackoffMs { get; init; }

    public IsolationLevel? IsolationLevel { get; init; }

    public bool? EnablePartitionEof { get; init; }

    public bool? CheckCrcs { get; init; }

    public bool? AllowAutoCreateTopics { get; init; }

    public string? Debug { get; init; }

    #endregion
}
