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
    private readonly List<Task> _tasks = new();
    private readonly RecordConsumerConfig _config;

    private IConsumer<byte[], byte[]>? _consumer;
    private CancellationTokenSource? _cts;
    private Func<ConsumeResult<byte[], byte[]>, ChannelWriter<(ConsumeResult<byte[], byte[]>, AckId?)>>? _channelWriterResolver;

    private bool _disposed;

    public RecordConsumer(RecordConsumerConfig config)
    {
        _config = config;
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

        _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

        if (ConsumeResultHandler is not null && _config.MaxDegreeOfParallelism > 0)
        {
            var channels = new Channel<(ConsumeResult<byte[], byte[]>, AckId?)>[_config.MaxDegreeOfParallelism];

            for (var i = 0; i < _config.MaxDegreeOfParallelism; i++)
            {
                var channel = Channel.CreateUnbounded<(ConsumeResult<byte[], byte[]>, AckId?)>();
                channels[i] = channel;
                _tasks.Add(HandleLoop(channel.Reader, ConsumeResultHandler, _cts.Token));
            }

            if (_config.MaxDegreeOfParallelism is 1)
            {
                _channelWriterResolver = _ => channels[0];
            }
            else if (MemoryPartitionKeyResolver is not null)
            {
                var partitionManger = new ValueBasedPartitionManager(_config.MaxDegreeOfParallelism);
                _channelWriterResolver = consumeResult =>
                {
                    var partitionKey = MemoryPartitionKeyResolver(consumeResult);
                    var partition = partitionManger.GetPartition(partitionKey);
                    return channels[partition];
                };
            }
            else
            {
                var partitionManger = new RoundRobinPartitionManager(_config.MaxDegreeOfParallelism);
                _channelWriterResolver = _ =>
                {
                    var partition = partitionManger.GetNextPartition();
                    return channels[partition];
                };
            }
        }

        _tasks.Add(ConsumeLoop(_consumer, _cts.Token));

        if (IsAutoCommitEnabled())
            _tasks.Add(CommitLoop(_consumer, _cts.Token));
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

        _channelWriterResolver = null;

        _cts?.Dispose();
        _cts = null;

        try
        {
            // Manually commit remaining offsets
            // to minimise duplicate messages.
            CommitOffsets(_consumer);
        }
        catch (Exception)
        {
            // Ignore.
        }

        _committedOffsets.Clear();

        foreach (var offsetManager in _offsetManagers.Values)
            offsetManager.Dispose();

        _offsetManagers.Clear();

        _consumer.Dispose();
        _consumer = null;
    }

    private Task ConsumeLoop(IConsumer<byte[], byte[]> consumer, CancellationToken token)
    {
        return RunLoop(
            async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(token);

                    if (ConsumeResultHandler is null || _channelWriterResolver is null)
                    {
                        MarkAsAcked(consumeResult);
                        continue;
                    }

                    AckId? ackId;
                    try
                    {
                        if (IsAutoOffsetStoreEnabled())
                        {
                            MarkAsAcked(consumeResult);
                            ackId = null;
                        }
                        else
                        {
                            ackId = await GetAckIdAsync(consumeResult, token);
                        }
                    }
                    catch (KafkaOffsetManagementException e)
                        when (e.ErrorCode is KafkaOffsetManagementErrorCode.OffsetOutOfOrder)
                    {
                        // Partition was revoked and assigned back.
                        // Some messages are redelivered therefore can be discarded.
                        continue;
                    }

                    var channelWriter = _channelWriterResolver(consumeResult);
                    await channelWriter.WriteAsync((consumeResult, ackId), token);
                }
            },
            token);
    }

    private Task HandleLoop(
        ChannelReader<(ConsumeResult<byte[], byte[]>, AckId?)> reader,
        Func<ConsumeResult<byte[], byte[]>, CancellationToken, Task> handle,
        CancellationToken token)
    {
        return RunLoop(
            async () =>
            {
                await foreach (var (consumeResult, ackId) in reader.ReadAllAsync(token))
                {
                    try
                    {
                        await handle(consumeResult, token);
                    }
                    catch (Exception)
                        when (Producer is not null)
                    {
                        await SendToDeadLetterTopicAsync(Producer, consumeResult, token);
                    }

                    if (ackId is not null)
                        Ack(consumeResult, ackId.Value);
                }
            },
            token);
    }

    private Task CommitLoop(IConsumer<byte[], byte[]> consumer, CancellationToken token)
    {
        return RunLoop(
           async () =>
           {
               var autoCommitIntervalMs = _config.AutoCommitIntervalMs ?? 5_000;

               while (!token.IsCancellationRequested)
               {
                   await Task.Delay(autoCommitIntervalMs, token);
                   CommitOffsets(consumer);
               }
           },
           token);
    }

    private Task RunLoop(Func<Task> loopTask, CancellationToken token)
    {
        return Task.Run(
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
                    when (ErrorHandler is not null)
                {
                    ErrorHandler(e);
                }
            },
            token);
    }

    private void MarkAsAcked(ConsumeResult<byte[], byte[]> consumeResult)
    {
        var offsetManager = GetOrCreateOffsetManager(consumeResult);
        offsetManager.MarkAsAcked(consumeResult.Offset);
    }

    private Task<AckId> GetAckIdAsync(ConsumeResult<byte[], byte[]> consumeResult, CancellationToken token)
    {
        var offsetManager = GetOrCreateOffsetManager(consumeResult);
        return offsetManager.GetAckIdAsync(consumeResult.Offset, token);
    }

    private void Ack(ConsumeResult<byte[], byte[]> consumeResult, AckId ackId)
    {
        var offsetManager = GetOffsetManager(consumeResult);
        offsetManager.Ack(ackId);
    }

    private OffsetManager GetOffsetManager(ConsumeResult<byte[], byte[]> consumeResult)
    {
        return _offsetManagers[consumeResult.TopicPartition];
    }

    private OffsetManager GetOrCreateOffsetManager(ConsumeResult<byte[], byte[]> consumeResult)
    {
        if (!_offsetManagers.TryGetValue(consumeResult.TopicPartition, out var offsetManager))
        {
            offsetManager = new OffsetManager(_config.MaxUncommittedMessagesPerMemoryPartition);
            _offsetManagers[consumeResult.TopicPartition] = offsetManager;
            _committedOffsets[consumeResult.TopicPartition] = Offset.Beginning;
        }

        return offsetManager;
    }

    private void CommitOffsets(IConsumer<byte[], byte[]> consumer)
    {
        var topicPartitionOffsets = new List<TopicPartitionOffset>();

        foreach (var (topicPartition, offsetManager) in _offsetManagers)
        {
            var commitOffset = offsetManager.GetCommitOffset();
            if (commitOffset is null || commitOffset <= _committedOffsets[topicPartition])
                continue;

            topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartition, commitOffset.Value));
            _committedOffsets[topicPartition] = commitOffset.Value;
        }

        consumer.Commit(topicPartitionOffsets);
    }

    private async Task SendToDeadLetterTopicAsync(
        IProducer<byte[], byte[]> producer,
        ConsumeResult<byte[], byte[]> consumeResult,
        CancellationToken token)
    {
        var deadLetterTopic = GetDeadLetterTopic(consumeResult.Topic);
        await producer.ProduceAsync(deadLetterTopic, consumeResult.Message, token);
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

    private bool IsAutoCommitEnabled()
    {
        return _config.EnableAutoCommit is null or true && _config.AutoCommitIntervalMs is not 0;
    }

    private bool IsAutoOffsetStoreEnabled()
    {
        return _config.EnableAutoOffsetStore is null or true;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Stop();

        _disposed = true;
    }
}
