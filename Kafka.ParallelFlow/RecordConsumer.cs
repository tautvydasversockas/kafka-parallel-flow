﻿using Confluent.Kafka;
using Kafka.OffsetManagement;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Kafka.ParallelFlow
{
    public sealed class RecordConsumer<TKey, TValue>
    {
        public IDeserializer<TKey>? KeyDeserializer { get; set; }
        public IDeserializer<TValue>? ValueDeserializer { get; set; }
        public Action<IConsumer<TKey, TValue>, Error>? ErrorHandler { get; set; }
        public Action<IConsumer<TKey, TValue>, LogMessage>? LogHandler { get; set; }
        public Action<IConsumer<TKey, TValue>, string>? StatisticsHandler { get; set; }
        public Func<ConsumeResult<TKey, TValue>, CancellationToken, Task>? ConsumeResultHandler { get; set; }
        public Func<ConsumeResult<TKey, TValue>, byte[]>? MemoryPartitionKeyResolver { get; set; }

        private readonly Dictionary<TopicPartition, OffsetManager> _offsetManagers = new();
        private readonly Channel<(ConsumeResult<TKey, TValue>, AckId)>[] _channels;
        private readonly RecordConsumerConfig _config;
        private readonly PartitionManager _partitionManager;

        private int _nextChannelIndex;
        private bool _isStarted;

        public RecordConsumer(RecordConsumerConfig config)
        {
            _config = config;
            _partitionManager = new PartitionManager(config.MaxDegreeOfParallelism);

            _channels = new Channel<(ConsumeResult<TKey, TValue>, AckId)>[config.MaxDegreeOfParallelism];
            for (var i = 0; i < config.MaxDegreeOfParallelism; i++)
                _channels[i] = Channel.CreateUnbounded<(ConsumeResult<TKey, TValue>, AckId)>();
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

            var tasks = new Task[_config.MaxDegreeOfParallelism + 2];
            int i;

            for (i = 0; i < _config.MaxDegreeOfParallelism; i++)
            {
                var reader = _channels[i].Reader;
                tasks[i] = StartHandleLoop(reader, compositeToken);
            }

            tasks[i++] = StartConsumeLoop(consumer, compositeToken);
            tasks[i] = StartCommitLoop(consumer, compositeToken);

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

        private Task StartConsumeLoop(IConsumer<TKey, TValue> consumer, CancellationToken token)
        {
            return Task.Run(
                async () =>
                {
                    try
                    {
                        while (!token.IsCancellationRequested)
                        {
                            var consumeResult = consumer.Consume(token);
                            var topicPartition = consumeResult.TopicPartition;

                            AckId ackId;

                            try
                            {
                                ackId = await GetAckIdAsync(consumeResult, token);
                            }
                            catch (KafkaOffsetManagementException e)
                                when (e.ErrorCode is KafkaOffsetManagementErrorCode.OffsetOutOfOrder)
                            {
                                // Partition was revoked and assigned back.
                                // Some messages are redelivered therefore can be discarded.
                                continue;
                            }

                            var channelWriter = GetChannelWriter(consumeResult);
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

        private Task StartHandleLoop(ChannelReader<(ConsumeResult<TKey, TValue>, AckId)> reader, CancellationToken token)
        {
            return Task.Run(
                async () =>
                {
                    try
                    {
                        await foreach (var (consumeResult, ackId) in reader.ReadAllAsync(token))
                        {
                            if (ConsumeResultHandler is not null)
                                await ConsumeResultHandler(consumeResult, token);

                            Ack(consumeResult, ackId);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore.
                    }
                },
                token);
        }

        private Task StartCommitLoop(IConsumer<TKey, TValue> consumer, CancellationToken token)
        {
            return Task.Run(
                async () =>
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
                },
                token);
        }

        private ChannelWriter<(ConsumeResult<TKey, TValue>, AckId)> GetChannelWriter(ConsumeResult<TKey, TValue> consumeResult)
        {
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

            return _channels[channelIndex].Writer;
        }

        private Task<AckId> GetAckIdAsync<TAckKey, TAckValue>(ConsumeResult<TAckKey, TAckValue> consumeResult, CancellationToken token)
        {
            if (!_offsetManagers.TryGetValue(consumeResult.TopicPartition, out var offsetManager))
            {
                offsetManager = new OffsetManager(_config.MaxUncommittedMessages);
                _offsetManagers[consumeResult.TopicPartition] = offsetManager;
            }

            return offsetManager.GetAckIdAsync(consumeResult.Offset, token);
        }

        private void Ack<TAckKey, TAckValue>(ConsumeResult<TAckKey, TAckValue> consumeResult, AckId ackId)
        {
            var offsetManager = _offsetManagers[consumeResult.TopicPartition];
            offsetManager.Ack(ackId);
        }

        private void CommitOffsets(IConsumer<TKey, TValue> consumer)
        {
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

            consumer.Commit(topicPartitionOffsets);
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
                Debug = _config.Debug,
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
    }
}
