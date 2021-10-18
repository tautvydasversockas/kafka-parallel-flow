# Kafka Parallel Flow

[![Build status](https://img.shields.io/github/workflow/status/tautvydasversockas/kafka-parallel-flow/publish-nuget)](https://github.com/tautvydasversockas/kafka-parallel-flow/actions/workflows/pipeline.yml)
[![NuGet downloads](https://img.shields.io/nuget/v/kafka.parallelflow.svg)](https://www.nuget.org/packages/Kafka.ParallelFlow/)
[![GitHub license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/tautvydasversockas/kafka-parallel-flow/blob/main/LICENSE)

This library provides a Kafka client for consuming messages in parallel from a single topic partition. 
Parallel consumption allows to process more data while at the same time improves CPU utilization.

This library is based on [confluent-kafka-dotnet] (https://github.com/confluentinc/confluent-kafka-dotnet) library.

## Installation

Available on [NuGet](https://www.nuget.org/packages/Kafka.ParallelFlow/)

```bash
dotnet add package Kafka.ParallelFlow
```

```powershell
PM> Install-Package Kafka.ParallelFlow
```

## Usage

```csharp
using Kafka.ParallelFLow;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        // TBD
    }
}
```
