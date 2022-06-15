using Confluent.Kafka;
using Kafka.OffsetManagement;
using Kafka.ParallelFlow.PartitionManagers;
using System.Threading.Channels;

namespace Kafka.ParallelFlow;

/// <summary>
///     Defines a high-level Apache Kafka consumer (with key and value deserialization).
/// </summary>
public sealed class RecordConsumer : IDisposable
{
    /// <summary>
    ///     Handles consumer error events.
    /// </summary>
    public Action<IConsumer<byte[], byte[]>, Error>? ErrorEventsHandler { get; set; }

    /// <summary>
    ///     Handles consumer exceptions.
    /// </summary>
    public Action<Exception>? ErrorHandler { get; set; }

    /// <summary>
    ///     Handles consumer information logs.
    /// </summary>
    public Action<IConsumer<byte[], byte[]>, LogMessage>? LogHandler { get; set; }

    /// <summary>
    ///     Handles consumer statistics.
    /// </summary>
    public Action<IConsumer<byte[], byte[]>, string>? StatisticsHandler { get; set; }

    /// <summary>
    ///     Handles consumed messages.
    ///     If not specified, message offsets will be commited instead.
    /// </summary>
    public Func<ConsumeResult<byte[], byte[]>, CancellationToken, Task>? ConsumeResultHandler { get; set; }

    /// <summary>
    ///     Resolves memory partition key. 
    ///     If not specified, partitions will be resolved in a round robin fashion.
    /// </summary>
    public Func<ConsumeResult<byte[], byte[]>, byte[]>? MemoryPartitionKeyResolver { get; set; }

    /// <summary>
    ///     Producer for producing messages to dead letter topics.
    /// </summary>
    public IProducer<byte[], byte[]>? Producer { get; set; }

    private readonly Dictionary<TopicPartition, OffsetManager> _offsetManagers = new();
    private readonly Dictionary<TopicPartition, Offset> _committedOffsets = new();
    private readonly List<Channel<(ConsumeResult<byte[], byte[]>, AckId?)>> _channels = new();
    private readonly List<Task> _tasks = new();
    private readonly RecordConsumerConfig _config;
    private readonly RoundRobinPartitionManager _roundRobinPartitionManager;
    private readonly ValueBasedPartitionManager _valueBasedPartitionManger;

    private IConsumer<byte[], byte[]>? _consumer;
    private CancellationTokenSource? _cts;

    private bool _disposed;

    public RecordConsumer(RecordConsumerConfig config)
    {
        _config = config;
        _roundRobinPartitionManager = new RoundRobinPartitionManager(config.MaxDegreeOfParallelism);
        _valueBasedPartitionManger = new ValueBasedPartitionManager(config.MaxDegreeOfParallelism);

        for (var i = 0; i < config.MaxDegreeOfParallelism; i++)
        {
            var channel = Channel.CreateUnbounded<(ConsumeResult<byte[], byte[]>, AckId?)>();
            _channels.Add(channel);
        }
    }

    /// <summary>
    ///     Starts consuming specified topic.
    /// </summary>
    public void Start(string topic, CancellationToken token = default)
    {
        Start(new[] { topic }, token);
    }

    /// <summary>
    ///     Starts consuming specified topics.
    /// </summary>
    public void Start(IEnumerable<string> topics, CancellationToken token = default)
    {
        if (_consumer is not null)
            throw new InvalidOperationException("Already started.");

        _consumer = BuildConsumer();
        _consumer.Subscribe(topics);

        _cts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(token, _cts.Token);
        var linkedToken = linkedCts.Token;

        foreach (var channel in _channels)
            StartLoop(() => HandleLoop(channel.Reader, linkedToken), linkedToken);

        StartLoop(() => ConsumeLoop(linkedToken), linkedToken);

        if (_config.EnableAutoCommit is null or true &&
            _config.AutoCommitIntervalMs is not 0)
        {
            var autoCommitIntervalMs = _config.AutoCommitIntervalMs ?? 5_000;
            StartLoop(() => CommitLoop(autoCommitIntervalMs, linkedToken), linkedToken);
        }
    }

    /// <summary>
    ///     Stops consumer.
    /// </summary>
    public void Stop(CancellationToken token = default)
    {
        if (_consumer is null)
            return;

        _cts?.Cancel();

        Task.WaitAll(_tasks.ToArray(), token);

        foreach (var task in _tasks)
            task.Dispose();

        _tasks.Clear();

        _cts?.Dispose();
        _cts = null;

        try
        {
            // Manually commit remaining offsets
            // to minimise duplicate messages.
            CommitOffsets();
        }
        catch (Exception)
        {
            // Ignore.
        }

        _consumer.Dispose();
        _consumer = null;
    }

    private void StartLoop(Func<Task> loopTask, CancellationToken token)
    {
        var task = Task.Run(
            async () =>
            {
                try
                {
                    await loopTask();
                }
                catch (OperationCanceledException)
                {
                    // Ignore.
                }
                catch (Exception e)
                {
                    ErrorHandler?.Invoke(e);
                }
            },
            token);

        _tasks.Add(task);
    }

    private async Task ConsumeLoop(CancellationToken token)
    {
        if (_consumer is null)
            throw new InvalidOperationException("Consumer is not initialised.");

        while (!token.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(token);
            await WriteToChannelAsync(consumeResult, token);
        }
    }

    private async Task HandleLoop(ChannelReader<(ConsumeResult<byte[], byte[]>, AckId?)> reader, CancellationToken token)
    {
        await foreach (var (consumeResult, ackId) in reader.ReadAllAsync(token))
        {
            await HandleAsync(consumeResult, token);

            if (ackId is not null)
                Ack(consumeResult.TopicPartition, ackId.Value);
        }
    }

    private async Task CommitLoop(int autoCommitInterval, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            await Task.Delay(autoCommitInterval, token);
            CommitOffsets();
        }
    }

    private async Task HandleAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken token)
    {
        if (ConsumeResultHandler is null)
            return;

        try
        {
            await ConsumeResultHandler(consumeResult, token);
        }
        catch (Exception)
            when (Producer is not null)
        {
            await SendToDeadLetterTopicAsync(consumeResult, token);
        }
    }

    private async Task WriteToChannelAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken token)
    {
        AckId? ackId;

        if (_config.EnableAutoOffsetStore is null or true)
        {
            MarkAsAcked(consumeResult.TopicPartitionOffset);
            ackId = null;
        }
        else
        {
            try
            {
                ackId = await GetAckIdAsync(consumeResult.TopicPartitionOffset, token);
            }
            catch (KafkaOffsetManagementException oe)
                when (oe.ErrorCode is KafkaOffsetManagementErrorCode.OffsetOutOfOrder)
            {
                // Partition was revoked and assigned back.
                // Some messages are redelivered therefore can be discarded.
                return;
            }
        }

        await WriteToChannelAsync(consumeResult, ackId, token);
    }

    private ValueTask WriteToChannelAsync(ConsumeResult<byte[], byte[]> consumeResult, AckId? ackId, CancellationToken token)
    {
        var channelIndex = GetNextChannelIndex(consumeResult);
        var writer = _channels[channelIndex].Writer;
        return writer.WriteAsync((consumeResult, ackId), token);
    }

    private int GetNextChannelIndex(ConsumeResult<byte[], byte[]> consumeResult)
    {
        if (MemoryPartitionKeyResolver is not null)
        {
            var partitionKey = MemoryPartitionKeyResolver(consumeResult);
            return _valueBasedPartitionManger.GetPartition(partitionKey);
        }

        return _roundRobinPartitionManager.GetNextPartition();
    }

    private void MarkAsAcked(TopicPartitionOffset topicPartitionOffset)
    {
        var offsetManager = GetOrCreateOffsetManager(topicPartitionOffset.TopicPartition);
        offsetManager.MarkAsAcked(topicPartitionOffset.Offset);
    }

    private Task<AckId> GetAckIdAsync(TopicPartitionOffset topicPartitionOffset, CancellationToken token)
    {
        var offsetManager = GetOrCreateOffsetManager(topicPartitionOffset.TopicPartition);
        return offsetManager.GetAckIdAsync(topicPartitionOffset.Offset, token);
    }

    private void Ack(TopicPartition topicPartition, AckId ackId)
    {
        var offsetManager = _offsetManagers[topicPartition];
        offsetManager.Ack(ackId);
    }

    private OffsetManager GetOrCreateOffsetManager(TopicPartition topicPartition)
    {
        if (_offsetManagers.TryGetValue(topicPartition, out var offsetManager))
            return offsetManager;

        offsetManager = new OffsetManager(_config.MaxUncommittedMessagesPerMemoryPartition);
        _offsetManagers[topicPartition] = offsetManager;
        _committedOffsets[topicPartition] = Offset.Beginning;
        return offsetManager;
    }

    private void CommitOffsets()
    {
        if (_consumer is null)
            throw new InvalidOperationException("Consumer is not initialised.");

        var topicPartitionOffsets = new List<TopicPartitionOffset>();

        foreach (var (topicPartition, offsetManager) in _offsetManagers)
        {
            var commitOffset = offsetManager.GetCommitOffset();
            if (commitOffset is null || commitOffset <= _committedOffsets[topicPartition])
                continue;

            _committedOffsets[topicPartition] = commitOffset.Value;
            var topicPartitionOffset = new TopicPartitionOffset(topicPartition, commitOffset.Value);
            topicPartitionOffsets.Add(topicPartitionOffset);
        }

        _consumer.Commit(topicPartitionOffsets);
    }

    private async Task SendToDeadLetterTopicAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken token)
    {
        if (Producer is null)
            throw new InvalidOperationException("Producer is not initialised.");

        var deadLetterTopic = GetDeadLetterTopic(consumeResult.Topic);
        await Producer.ProduceAsync(deadLetterTopic, consumeResult.Message, token);
    }

    private string GetDeadLetterTopic(string topic)
    {
        return _config.DeadLetterTopic ?? $"{topic}__{_config.GroupId}__dlt";
    }

    private IConsumer<byte[], byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig();

        foreach (var (key, value) in _config)
            config.Set(key, value);

        config.EnableAutoCommit = false;
        config.AutoCommitIntervalMs = 0;
        config.EnableAutoOffsetStore = false;

        var builder = new ConsumerBuilder<byte[], byte[]>(config);

        if (LogHandler is not null)
            builder.SetLogHandler(LogHandler);

        if (ErrorHandler is not null)
            builder.SetErrorHandler(ErrorEventsHandler);

        if (StatisticsHandler is not null)
            builder.SetStatisticsHandler(StatisticsHandler);

        return builder.Build();
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Stop();

        foreach (var offsetManager in _offsetManagers.Values)
            offsetManager.Dispose();

        _disposed = true;
    }
}
