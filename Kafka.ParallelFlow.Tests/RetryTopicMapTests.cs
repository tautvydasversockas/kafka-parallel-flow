namespace Kafka.ParallelFlow.Tests;

public sealed class RetryTopicMapTests
{
    [Fact]
    public void Getting_dead_letter_topic()
    {
        var sut = new RetryTopicMap("cg");

        var deadLetterTopic = sut.GetDeadLetterTopic("test");

        deadLetterTopic.Should().Be("test__cg__dlt");
    }
}
