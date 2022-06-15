# Kafka Parallel Flow

[![Build status](https://img.shields.io/github/workflow/status/tautvydasversockas/kafka-parallel-flow/publish-nuget)](https://github.com/tautvydasversockas/kafka-parallel-flow/actions/workflows/pipeline.yml)
[![NuGet downloads](https://img.shields.io/nuget/v/kafka.parallelflow.svg)](https://www.nuget.org/packages/Kafka.ParallelFlow/)
[![GitHub license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/tautvydasversockas/kafka-parallel-flow/blob/main/LICENSE)

This library provides a Kafka client for consuming messages in parallel from a single topic partition. 
Parallel consumption allows to process more data while at the same time improves CPU utilization.

This library is based on [confluent-kafka-dotnet](https://github.com/confluentinc/confluent-kafka-dotnet) library.

## Installation

Available on [NuGet](https://www.nuget.org/packages/Kafka.ParallelFlow/)

```bash
dotnet add package Kafka.ParallelFlow
```

```powershell
PM> Install-Package Kafka.ParallelFlow
```

## Usage

### Parallel consumer example

```csharp
using Kafka.ParallelFlow;

var consumerConfig = new RecordConsumerConfig
{
    GroupId = "group-id-1",
    BootstrapServers = "localhost:9092",
    // Messages will be handled in parallel by up to 10 threads.
    MaxDegreeOfParallelism = 10,
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 5_000,
    // If EnableAutoOffsetStore is true, messages will be stored for commit before they are handled.
    // If EnableAutoOffsetStore is false, messages will be stored for commit after they are handled.
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new RecordConsumer(consumerConfig);

// Messages with the same resolved key are handled in order.
// If `MemoryPartitionKeyResolver` is not set, no order 
// guarantees are provided - memory partitions are chosen in 
// round robin fashion.
consumer.MemoryPartitionKeyResolver = consumeResult => consumeResult.Message.Key;

consumer.ErrorHandler = ex => Environment.Exit(1);

consumer.ConsumeResultHandler = (consumeResult, _) => 
{
    Console.WriteLine(consumeResult.TopicPartitionOffset.ToString());
    return Task.CompletedTask;
};

// Starting non-blocking consumption.
consumer.Start("test-topic");

Console.ReadKey();
```

### Dead letter topic example

```csharp
using Kafka.ParallelFlow;

var consumerConfig = new RecordConsumerConfig
{
    GroupId = "group-id-1",
    BootstrapServers = "localhost:9092",
    MaxDegreeOfParallelism = 10,
    EnableAutoCommit = true,
    AutoCommitIntervalMs = 5_000,
    EnableAutoOffsetStore = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    // Default dead letter topic name is <topic>__<consumer-group-id>__dlt.
    // It can be overriden by setting DeadLetterTopic value.
    DeadLetterTopic = "dead-letters"
};

using var consumer = new RecordConsumer(consumerConfig);

consumer.ErrorHandler = ex => Environment.Exit(1);

var producerConfig = new ProducerConfig
{
    BootstrapServers = BootstrapServers
};

using var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();

// Setting producer enables dead letter topic functionality. 
// If an error occurs while handling message, 
// the message will be produced to a dead letter topic. 
consumer.Producer = producer;

consumer.ConsumeResultHandler = (consumeResult, _) => 
{
    Console.WriteLine(consumeResult.TopicPartitionOffset.ToString());
    return Task.CompletedTask;
};

consumer.Start("test-topic");

Console.ReadKey();
```

## Support

<a href="https://www.buymeacoffee.com/tautvydasverso"> 
    <img align="left" src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" height="50" width="210"  alt="tautvydasverso" />
</a>
