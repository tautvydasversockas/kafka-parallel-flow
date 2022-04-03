namespace Kafka.ParallelFlow.PartitionManagers;

internal sealed class ValueBasedPartitionManager
{
    private readonly int _numberOfPartitions;

    public ValueBasedPartitionManager(int numberOfPartitions)
    {
        if (numberOfPartitions < 1)
            throw new ArgumentException("Number of partitions must be greater than 0.", nameof(numberOfPartitions));

        _numberOfPartitions = numberOfPartitions;
    }

    public int GetPartition(Span<byte> value)
    {
        var hash = Math.Abs(Hash(value));
        return (int)(hash % _numberOfPartitions);
    }

    private static long Hash(Span<byte> value)
    {
        var hash = 14695981039346656037;
        unchecked
        {
            foreach (var b in value)
            {
                hash ^= b;
                hash *= 1099511628211;
            }

            return (long)hash;
        }
    }
}
