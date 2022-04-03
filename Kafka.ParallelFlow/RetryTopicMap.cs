namespace Kafka.ParallelFlow;

internal sealed class RetryTopicMap
{
    private readonly Dictionary<string, string> _deadLetterTopics = new();
    private readonly string _consumerGroupId;

    public RetryTopicMap(string consumerGroupId)
    {
        consumerGroupId = consumerGroupId.Trim();

        if (consumerGroupId.Length is 0)
            throw new ArgumentException("Consumer group id is required.", nameof(consumerGroupId));

        _consumerGroupId = consumerGroupId;
    }

    public string GetDeadLetterTopic(string topic)
    {
        if (_deadLetterTopics.TryGetValue(topic, out var deadLetterTopic))
            return deadLetterTopic;

        deadLetterTopic = $"{topic}__{_consumerGroupId}__dlt";
        _deadLetterTopics[topic] = deadLetterTopic;
        return deadLetterTopic;
    }
}
