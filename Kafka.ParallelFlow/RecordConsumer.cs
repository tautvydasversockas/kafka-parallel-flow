using Confluent.Kafka;
using Kafka.OffsetManager;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Kafka.ParallelFlow
{
    public sealed class RecordConsumer<TKey, TValue> : IDisposable
    {
        public IDeserializer<TKey>? KeyDeserializer { get; set; }
        public IDeserializer<TValue>? ValueDeserializer { get; set; }
        public Action<IConsumer<TKey, TValue>, Error>? ErrorHandler { get; set; }
        public Action<IConsumer<TKey, TValue>, LogMessage>? LogHandler { get; set; }
        public Action<IConsumer<TKey, TValue>, string>? StatisticsHandler { get; set; }
        public Func<ConsumeResult<TKey, TValue>, byte[]>? MemoryPartitionKeyResolver { get; set; }

        private readonly Dictionary<TopicPartition, OffsetManager.OffsetManager> _offsetManagers = new();
        private readonly Task[] _tasks;
        private readonly Channel<(ConsumeResult<TKey, TValue>, AckId)>[] _channels;
        private readonly RecordConsumerConfig _config;
        private readonly PartitionManager _partitionManager;

        private IConsumer<TKey, TValue>? _consumer;
        private int _nextChannelIndex;

        public RecordConsumer(RecordConsumerConfig config)
        {
            _config = config;
            _partitionManager = new PartitionManager(config.MaxDegreeOfParallelism);

            _tasks = new Task[config.MaxDegreeOfParallelism + 2];

            _channels = new Channel<(ConsumeResult<TKey, TValue>, AckId)>[config.MaxDegreeOfParallelism];
            for (var i = 0; i < config.MaxDegreeOfParallelism; i++)
                _channels[i] = Channel.CreateUnbounded<(ConsumeResult<TKey, TValue>, AckId)>();
        }

        public async Task Start(
            IReadOnlyCollection<string> topics,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> consumeResultHandler,
            CancellationToken token = default)
        {
            if (_consumer is not null)
                throw new InvalidOperationException("Already started.");

            var errorCts = new CancellationTokenSource();
            var compositeCts = CancellationTokenSource.CreateLinkedTokenSource(token, errorCts.Token);
            var compositeTOken = compositeCts.Token;

            _consumer = BuildConsumer();
            _consumer.Subscribe(topics);

            int i;

            for (i = 0; i < _config.MaxDegreeOfParallelism; i++)
            {
                var reader = _channels[i].Reader;
                _tasks[i] = StartHandleLoop(reader, consumeResultHandler, token);
            }

            _tasks[i++] = StartConsumeLoop(token);
            _tasks[i] = StartCommitLoop(token);

            var firstCompletedTask = await Task.WhenAny(_tasks);
            if (firstCompletedTask.IsFaulted)
                errorCts.Cancel();

            await Task.WhenAll(_tasks);
        }

        private Task StartConsumeLoop(CancellationToken token)
        {
            if (_consumer is null)
                throw new InvalidOperationException("Consumer not started.");

            return Task.Run(
                async () =>
                {
                    try
                    {
                        while (!token.IsCancellationRequested)
                        {
                            var consumeResult = _consumer.Consume(token);
                            var topicPartition = consumeResult.TopicPartition;

                            if (!_offsetManagers.TryGetValue(topicPartition, out var offsetManager))
                            {
                                offsetManager = new OffsetManager.OffsetManager(_config.MaxUncommittedMessages);
                                _offsetManagers[topicPartition] = offsetManager;
                            }

                            var ackId = await offsetManager.GetAckIdAsync(consumeResult.Offset, token);

                            int channelIndex;

                            if (MemoryPartitionKeyResolver is null)
                            {
                                channelIndex = _nextChannelIndex;
                                _nextChannelIndex = _nextChannelIndex == _channels.Length - 1
                                    ? 0
                                    : _nextChannelIndex + 1;
                            }
                            else
                            {
                                var memoryPartitionKey = MemoryPartitionKeyResolver(consumeResult);
                                channelIndex = _partitionManager.GetPartition(memoryPartitionKey);
                            }

                            var channelWriter = _channels[channelIndex].Writer;
                            await channelWriter.WriteAsync((consumeResult, ackId), token);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore.
                    }
                },
                token);
        }

        private Task StartHandleLoop(
            ChannelReader<(ConsumeResult<TKey, TValue>, AckId)> reader,
            Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> consumeResultHandler,
            CancellationToken token)
        {
            if (_consumer is null)
                throw new InvalidOperationException("Consumer not started.");

            return Task.Run(
                async () =>
                {
                    try
                    {
                        await foreach (var (consumeResult, ackId) in reader.ReadAllAsync(token))
                        {
                            await consumeResultHandler(consumeResult, token);
                            var offsetManager = _offsetManagers[consumeResult.TopicPartition];
                            offsetManager.Ack(ackId);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore.
                    }
                },
                token);
        }

        private Task StartCommitLoop(CancellationToken token)
        {
            if (_consumer is null)
                throw new InvalidOperationException("Consumer not started.");

            return Task.Run(
                async () =>
                {
                    try
                    {
                        while (!token.IsCancellationRequested)
                        {
                            await Task.Delay(_config.CommitIntervalMs, token);

                            var topicPartitionOffsets = new List<TopicPartitionOffset>();

                            foreach (var (topicPartition, offsetManager) in _offsetManagers)
                            {
                                var commitOffset = offsetManager.GetCommitOffset();

                                if (commitOffset is not null)
                                {
                                    var topicPartitionOffset = new TopicPartitionOffset(topicPartition, commitOffset.Value);
                                    topicPartitionOffsets.Add(topicPartitionOffset);
                                }
                            }

                            _consumer.Commit(topicPartitionOffsets);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore.
                    }
                },
                token);
        }

        private IConsumer<TKey, TValue> BuildConsumer()
        {
            var config = new ConsumerConfig
            {
                GroupId = _config.GroupId,
                BootstrapServers = _config.BootstrapServers,
                SocketNagleDisable = _config.SocketNagleDisable,
                AutoOffsetReset = _config.AutoOffsetReset,
                AllowAutoCreateTopics = _config.AllowAutoCreateTopics,
                CheckCrcs = _config.CheckCrcs,
                SessionTimeoutMs = _config.SessionTimeoutMs,
                HeartbeatIntervalMs = _config.HeartbeatIntervalMs,
                CoordinatorQueryIntervalMs = _config.CoordinatorQueryIntervalMs,
                MaxPollIntervalMs = _config.MaxPollIntervalMs,
                QueuedMinMessages = _config.QueuedMinMessages,
                QueuedMaxMessagesKbytes = _config.QueuedMaxMessagesKbytes,
                FetchWaitMaxMs = _config.FetchWaitMaxMs,
                MaxPartitionFetchBytes = _config.MaxPartitionFetchBytes,
                FetchMaxBytes = _config.FetchMaxBytes,
                FetchMinBytes = _config.FetchMinBytes,
                FetchErrorBackoffMs = _config.FetchErrorBackoffMs,
                IsolationLevel = _config.IsolationLevel,
                EnablePartitionEof = _config.EnablePartitionEof,
                EnableAutoOffsetStore = false,
                EnableAutoCommit = false,
            };

            var builder = new ConsumerBuilder<TKey, TValue>(config);

            if (KeyDeserializer is not null)
                builder.SetKeyDeserializer(KeyDeserializer);

            if (ValueDeserializer is not null)
                builder.SetValueDeserializer(ValueDeserializer);

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
            _consumer?.Dispose();

            foreach (var task in _tasks)
                task.Dispose();
        }
    }
}
