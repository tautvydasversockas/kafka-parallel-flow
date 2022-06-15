using FluentAssertions;
using Kafka.ParallelFlow.PartitionManagers;
using System.Text;
using Xunit;

namespace Kafka.ParallelFlow.Tests.PartitionManagers;

public sealed class ValueBasedPartitionManagerTests
{
    [Fact]
    public void Getting_partition_from_the_same_value()
    {
        const string valueAsText = "value";
        var valueA = Encoding.UTF8.GetBytes(valueAsText);
        var valueB = Encoding.UTF8.GetBytes(valueAsText);
        var sut = new ValueBasedPartitionManager(100);

        var partitionA = sut.GetPartition(valueA);
        var partitionB = sut.GetPartition(valueB);

        partitionA.Should().Be(partitionB);
    }

    [Fact]
    public void Getting_partition_from_a_different_value()
    {
        var valueA = Encoding.UTF8.GetBytes("valueA");
        var valueB = Encoding.UTF8.GetBytes("valueB");
        var sut = new ValueBasedPartitionManager(100);

        var partitionA = sut.GetPartition(valueA);
        var partitionB = sut.GetPartition(valueB);

        partitionA.Should().NotBe(partitionB);
    }
}
