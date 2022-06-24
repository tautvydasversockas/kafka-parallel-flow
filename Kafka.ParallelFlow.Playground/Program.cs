using Confluent.Kafka;
using Kafka.ParallelFlow;
using System.Text;

var Topic = "my-topic";
var Group = "my-group";
var BootstrapServers = "localhost:9092";

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (s, e) =>
{
    Console.WriteLine("Canceling...");
    cts.Cancel();
    e.Cancel = true;
};

using var consumer = CreateConsumer();
using var producer = CreateProducer();

consumer.Start(Topic, cts.Token);

while (!cts.Token.IsCancellationRequested)
{
    var message = new Message<byte[], byte[]>
    {
        Key = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")),
        Value = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"))
    };

    await producer.ProduceAsync(Topic, message, cts.Token);
    await Task.Delay(100, cts.Token);
}


RecordConsumer CreateConsumer()
{
    var config = new RecordConsumerConfig
    {
        GroupId = Group,
        BootstrapServers = BootstrapServers,
        MaxDegreeOfParallelism = 10,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = true,
        EnableAutoOffsetStore = false
    };

    var consumer = new RecordConsumer(config);

    consumer.ErrorEventsHandler = (_, e) => Console.WriteLine($"Consumer ErrorEventsHandler: {e}");
    consumer.ErrorHandler = e => Console.WriteLine($"Consumer ErrorHandler: {e.Message}");
    consumer.LogHandler = (_, log) => Console.WriteLine($"Consumer LogHandler: {log}");

    consumer.MemoryPartitionKeyResolver = cr => cr.Message.Key;

    consumer.ConsumeResultHandler = (cr, ct) =>
    {
        Console.WriteLine($"ConsumeResultHandler: {cr.TopicPartitionOffset}");
        return Task.CompletedTask;
    };

    return consumer;
}

IProducer<byte[], byte[]> CreateProducer()
{
    var config = new ProducerConfig
    {
        BootstrapServers = BootstrapServers
    };

    var producerBuilder = new ProducerBuilder<byte[], byte[]>(config);

    producerBuilder.SetErrorHandler((_, e) => Console.WriteLine($"Producer ErrorEventsHandler: {e}"));
    producerBuilder.SetLogHandler((_, log) => Console.WriteLine($"Producer LogHandler: {log}"));

    return producerBuilder.Build();
}
