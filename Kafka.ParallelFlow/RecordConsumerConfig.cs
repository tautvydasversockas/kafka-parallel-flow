using Confluent.Kafka;

namespace Kafka.ParallelFlow
{
    public sealed class RecordConsumerConfig
    {
        #region `Kafka.ParallelFlow` config values

        public int AutoCommitIntervalMs { get; set; } = 5_000;

        public int MaxDegreeOfParallelism { get; set; } = 1;

        public int MaxUncommittedMessagesPerMemoryPartition { get; set; } = 10_000;

        #endregion

        #region `Confluent.Kafka` config values

        public string? GroupId { get; set; }

        public string? BootstrapServers { get; set; }

        public bool? SocketNagleDisable { get; set; }

        public AutoOffsetReset? AutoOffsetReset { get; set; }

        public int? SessionTimeoutMs { get; set; }

        public int? HeartbeatIntervalMs { get; set; }

        public int? CoordinatorQueryIntervalMs { get; set; }

        public int? MaxPollIntervalMs { get; set; }

        public int? QueuedMinMessages { get; set; }

        public int? QueuedMaxMessagesKbytes { get; set; }

        public int? FetchWaitMaxMs { get; set; }

        public int? MaxPartitionFetchBytes { get; set; }

        public int? FetchMaxBytes { get; set; }

        public int? FetchMinBytes { get; set; }

        public int? FetchErrorBackoffMs { get; set; }

        public IsolationLevel? IsolationLevel { get; set; }

        public bool? EnablePartitionEof { get; set; }

        public bool? CheckCrcs { get; set; }

        public bool? AllowAutoCreateTopics { get; set; }

        public string? Debug { get; set; }

        #endregion
    }
}
