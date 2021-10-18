using Confluent.Kafka;

namespace Kafka.ParallelFlow
{
    public sealed class RecordConsumerConfig
    {
        public int AutoCommitIntervalMs { get; set; } = 5_000;

        public bool SocketNagleDisable { get; set; } = false;

        public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Latest;

        public int SessionTimeoutMs { get; set; } = 45_000;

        public int HeartbeatIntervalMs { get; set; } = 3_000;

        public string GroupId { get; set; } = string.Empty;

        public string BootstrapServers { get; set; } = string.Empty;

        public int MaxDegreeOfParallelism { get; set; } = 1;

        public int MaxUncommittedMessages { get; set; } = 10_000;

        public int CoordinatorQueryIntervalMs { get; set; } = 600_000;

        public int MaxPollIntervalMs { get; set; } = 300_000;

        public int QueuedMinMessages { get; set; } = 100_000;

        public int QueuedMaxMessagesKbytes { get; set; } = 65_536;

        public int FetchWaitMaxMs { get; set; } = 500;

        public int MaxPartitionFetchBytes { get; set; } = 1_048_576;

        public int FetchMaxBytes { get; set; } = 52_428_800;

        public int FetchMinBytes { get; set; } = 1;

        public int FetchErrorBackoffMs { get; set; } = 500;

        public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

        public bool EnablePartitionEof { get; set; } = false;

        public bool CheckCrcs { get; set; } = false;

        public bool AllowAutoCreateTopics { get; set; } = false;

        public string Debug { get; set; } = string.Empty;
    }
}
