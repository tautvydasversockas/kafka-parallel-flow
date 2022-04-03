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

    #region `Kafka.ParallelFlow` config values

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

    public string? DeadLetterTopicName { get; set; }


    #endregion

    #region `Confluent.Kafka` config override values

    /// <summary>
    ///     The frequency in milliseconds that the consumer offsets are committed (written) to offset storage.
    ///
    ///     default: 5000
    ///     importance: medium
    /// </summary>
    public new int AutoCommitIntervalMs { get; init; } = 5_000;

    /// <summary>
    ///     Automatically store offset of last message provided to application. The offset store is an in-memory store of the next offset to (auto-)commit for each partition.
    ///
    ///     default: false
    ///     importance: high
    /// </summary>
    public readonly new bool EnableAutoOffsetStore = false;

    /// <summary>
    ///     Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign().
    ///
    ///     default: false
    ///     importance: high
    /// </summary>
    public readonly new bool EnableAutoCommit = false;

    #endregion
}
