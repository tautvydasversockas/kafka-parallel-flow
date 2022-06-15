namespace Kafka.ParallelFlow.PartitionManagers;

internal sealed class RoundRobinPartitionManager
{
    private readonly int _numberOfPartitions;
    private int _prevPartition = -1;

    public RoundRobinPartitionManager(int numberOfPartitions)
    {
        if (numberOfPartitions < 1)
            throw new ArgumentException("Number of partitions must be greater than 0.", nameof(numberOfPartitions));

        _numberOfPartitions = numberOfPartitions;
    }

    public int GetNextPartition()
    {
        return _prevPartition = _prevPartition + 1 == _numberOfPartitions ? 0 : _prevPartition + 1;
    }
}
