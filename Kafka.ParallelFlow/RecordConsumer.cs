using Kafka.ParallelFlow.PartitionManagers;

namespace Kafka.ParallelFlow;

/// <summary>
///     Defines a high-level Apache Kafka consumer (with key and value deserialization).
/// </summary>
public sealed class RecordConsumer : IDisposable
{
    public Action<IConsumer<byte[], byte[]>, Error>? ErrorHandler { get; set; }
    public Action<IConsumer<byte[], byte[]>, LogMessage>? LogHandler { get; set; }
    public Action<IConsumer<byte[], byte[]>, string>? StatisticsHandler { get; set; }
    public Func<ConsumeException, CancellationToken, Task>? DeserializationErrorHandler { get; set; }
    public Func<ConsumeResult<byte[], byte[]>, CancellationToken, Task>? ConsumeResultHandler { get; set; }
    public Func<ConsumeResult<byte[], byte[]>, byte[]>? MemoryPartitionKeyResolver { get; set; }
    public IProducer<byte[], byte[]>? Producer { get; set; }

    private readonly Dictionary<TopicPartition, OffsetManager> _offsetManagers = new();
    private readonly Dictionary<TopicPartition, Offset> _committedOffsets = new();
    private readonly Channel<(ConsumeResult<byte[], byte[]>, AckId)>[] _channels;
    private readonly RecordConsumerConfig _config;
    private readonly RoundRobinPartitionManager _roundRobinPartitionManager;
    private readonly ValueBasedPartitionManager _valueBasedPartitionManger;
    private readonly RetryTopicMap _retryTopicMap;

    private bool _isStarted;

    public RecordConsumer(RecordConsumerConfig config)
    {
        _config = config;
        _roundRobinPartitionManager = new RoundRobinPartitionManager(config.MaxDegreeOfParallelism);
        _valueBasedPartitionManger = new ValueBasedPartitionManager(config.MaxDegreeOfParallelism);
        _retryTopicMap = new RetryTopicMap(config.GroupId);

        _channels = new Channel<(ConsumeResult<byte[], byte[]>, AckId)>[config.MaxDegreeOfParallelism];
        for (var i = 0; i < config.MaxDegreeOfParallelism; i++)
            _channels[i] = Channel.CreateUnbounded<(ConsumeResult<byte[], byte[]>, AckId)>();
    }

    public Task Start(string topic, CancellationToken token = default)
    {
        return Start(new[] { topic }, token);
    }

    public async Task Start(IReadOnlyCollection<string> topics, CancellationToken token = default)
    {
        _isStarted = _isStarted
            ? throw new InvalidOperationException("Already started.")
            : true;

        var errorCts = new CancellationTokenSource();
        var compositeCts = CancellationTokenSource.CreateLinkedTokenSource(token, errorCts.Token);
        var compositeToken = compositeCts.Token;

        var consumer = BuildConsumer();
        consumer.Subscribe(topics);

        var consumerTaskCount = _config.MaxDegreeOfParallelism;
        var totalTaskCount = consumerTaskCount + 2;

        var tasks = new Task[totalTaskCount];
        int i;

        for (i = 0; i < consumerTaskCount; i++)
        {
            var reader = _channels[i].Reader;
            tasks[i] = Task.Run(() => HandleLoop(reader, compositeToken), compositeToken);
        }

        tasks[i++] = Task.Run(() => ConsumeLoop(consumer, compositeToken), compositeToken);
        tasks[i] = Task.Run(() => CommitLoop(consumer, compositeToken), compositeToken);

        var firstCompletedTask = await Task.WhenAny(tasks);
        if (firstCompletedTask.IsFaulted)
            errorCts.Cancel();

        try
        {
            await Task.WhenAll(tasks);
        }
        finally
        {
            foreach (var task in tasks)
                task.Dispose();

            CommitOffsets(consumer);
            consumer.Dispose();

            _isStarted = false;
        }
    }

    private async Task ConsumeLoop(IConsumer<byte[], byte[]> consumer, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(token);

                AckId ackId;

                try
                {
                    ackId = await GetAckIdAsync(consumeResult.TopicPartitionOffset, token);
                }
                catch (KafkaOffsetManagementException oe)
                    when (oe.ErrorCode is KafkaOffsetManagementErrorCode.OffsetOutOfOrder)
                {
                    // Partition was revoked and assigned back.
                    // Some messages are redelivered therefore can be discarded.
                    continue;
                }

                await WriteToChannelAsync(consumeResult, ackId, token);
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore.
        }
    }

    private async Task HandleLoop(ChannelReader<(ConsumeResult<byte[], byte[]>, AckId)> reader, CancellationToken token)
    {
        try
        {
            await foreach (var (consumeResult, ackId) in reader.ReadAllAsync(token))
            {
                if (ConsumeResultHandler is not null)
                {
                    try
                    {
                        await ConsumeResultHandler(consumeResult, token);
                    }
                    catch (Exception)
                        when (Producer is not null)
                    {
                        await SendToDeadLetterTopicAsync(Producer, consumeResult, token);
                    }
                }

                Ack(consumeResult.TopicPartition, ackId);
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore.
        }
    }

    private async Task CommitLoop(IConsumer<byte[], byte[]> consumer, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(_config.AutoCommitIntervalMs, token);
                CommitOffsets(consumer);
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore.
        }
    }

    private ValueTask WriteToChannelAsync(ConsumeResult<byte[], byte[]> consumeResult, AckId ackId, CancellationToken token)
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

    private Task<AckId> GetAckIdAsync(TopicPartitionOffset topicPartitionOffset, CancellationToken token)
    {
        if (!_offsetManagers.TryGetValue(topicPartitionOffset.TopicPartition, out var offsetManager))
        {
            offsetManager = new OffsetManager(_config.MaxUncommittedMessagesPerMemoryPartition);
            _offsetManagers[topicPartitionOffset.TopicPartition] = offsetManager;
            _committedOffsets[topicPartitionOffset.TopicPartition] = Offset.Beginning;
        }

        return offsetManager.GetAckIdAsync(topicPartitionOffset.Offset, token);
    }

    private void Ack(TopicPartition topicPartition, AckId ackId)
    {
        var offsetManager = _offsetManagers[topicPartition];
        offsetManager.Ack(ackId);
    }

    private void CommitOffsets(IConsumer<byte[], byte[]> consumer)
    {
        var commitTopicPartitionOffsets = new List<TopicPartitionOffset>();

        foreach (var (topicPartition, offsetManager) in _offsetManagers)
        {
            var commitOffset = offsetManager.GetCommitOffset();

            if (commitOffset is not null && commitOffset > _committedOffsets[topicPartition])
            {
                _committedOffsets[topicPartition] = commitOffset.Value;
                var commitTopicPartitionOffset = new TopicPartitionOffset(topicPartition, commitOffset.Value);
                commitTopicPartitionOffsets.Add(commitTopicPartitionOffset);
            }
        }

        consumer.Commit(commitTopicPartitionOffsets);
    }

    private Task SendToDeadLetterTopicAsync(IProducer<byte[], byte[]> producer, ConsumeResult<byte[], byte[]> consumeResult, CancellationToken token)
    {
        var deadLetterTopic = _retryTopicMap.GetDeadLetterTopic(consumeResult.Topic);
        return producer.ProduceAsync(deadLetterTopic, consumeResult.Message, token);
    }

    private IConsumer<byte[], byte[]> BuildConsumer()
    {
        var builder = new ConsumerBuilder<byte[], byte[]>(_config);

        if (LogHandler is not null)
            builder.SetLogHandler(LogHandler);

        if (ErrorHandler is not null)
            builder.SetErrorHandler(ErrorHandler);

        if (StatisticsHandler is not null)
            builder.SetStatisticsHandler(StatisticsHandler);

        return builder.Build();
    }

    public void Dispose()
    {
        foreach (var offsetManager in _offsetManagers.Values)
            offsetManager.Dispose();
    }
}
