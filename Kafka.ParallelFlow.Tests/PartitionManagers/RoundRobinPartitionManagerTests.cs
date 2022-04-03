namespace Kafka.ParallelFlow.Tests.PartitionManagers;

public sealed class RoundRobinPartitionManagerTests
{
    [Theory]
    [InlineData(3, 1, 0)]
    [InlineData(3, 2, 1)]
    [InlineData(3, 3, 2)]
    [InlineData(3, 4, 0)]
    public void Getting_partition(int numberOfPartitions, int partitionGetCount, int expectedPartition)
    {
        var sut = new RoundRobinPartitionManager(numberOfPartitions);

        var partition = (int?)null;
        for (var i = 0; i < partitionGetCount; i++)
            partition = sut.GetNextPartition();

        partition.Should().Be(expectedPartition);
    }
}
