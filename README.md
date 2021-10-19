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
using System;
using Kafka.ParallelFlow;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => 
        {
            e.Cancel = true;
            cts.Cancel();
        };

        var config = new RecordConsumerConfig
        {
            GroupId = "group-id-1",
            BootstrapServers = "localhost:9092",
            // Messages will be handled in parallel by up to 10 threads.
            MaxDegreeOfParallelism = 10
        };

        using var consumer = new RecordConsumer<byte[], byte[]>(config)
        {
            // Messages with the same resolved key are handled in order.
            // If `MemoryPartitionKeyResolver` is not set, no order 
            // guarantees are provided.
            MemoryPartitionKeyResolver = cr => cr.Message.Key,
            ConsumeResultHandler = (cr, ct) => 
            {
                Console.WriteLine(cr.TopicPartitionOffset.ToString());
                return Task.CompletedTask;
            }
        }

        await consumer.Start("test-topic", cts.Token)
    }
}
```
