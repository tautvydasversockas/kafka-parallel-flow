using Confluent.Kafka;

namespace Kafka.ParallelFlow;

/// <summary>
///     Record consumer configuration properties.
/// </summary>
public sealed class RecordConsumerConfig : ConsumerConfig
{
    /// <summary>
    ///     Initialize a new empty <see cref="RecordConsumerConfig" /> instance.
    /// </summary>
    public RecordConsumerConfig() : base() { }

    /// <summary>
    ///     Initialize a new <see cref="RecordConsumerConfig" /> instance wrapping
    ///     an existing <see cref="ConsumerConfig" /> instance.
    ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
    /// </summary>
    public RecordConsumerConfig(ConsumerConfig config) : base(config) { }

    /// <summary>
    ///     Initialize a new <see cref="RecordConsumerConfig" /> instance wrapping
    ///     an existing key/value pair collection.
    ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
    /// </summary>
    public RecordConsumerConfig(IDictionary<string, string> config) : base(config) { }

    /// <summary>
    ///     The max number of consumer threads.
    ///
    ///     default: 1
    ///     importance: high
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = 1;

    /// <summary>
    ///     The max number of uncommitted messages per memory partition.
    ///
    ///     default: 10000
    ///     importance: high
    /// </summary>
    public int MaxUncommittedMessagesPerMemoryPartition { get; init; } = 10_000;

    /// <summary>
    ///     The max number of uncommitted messages per memory partition.
    ///
    ///     default: {Topic}__{GroupId}__dlt
    ///     importance: low
    /// </summary>
    public string? DeadLetterTopic { get; set; }
}
