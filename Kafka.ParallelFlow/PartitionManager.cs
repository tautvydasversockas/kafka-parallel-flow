using System;

namespace Kafka.ParallelFlow
{
    internal sealed class PartitionManager
    {
        private readonly int _numberOfPartitions;

        public PartitionManager(int numberOfPartitions)
        {
            if (numberOfPartitions < 1)
                throw new ArgumentException(
                    "Number of partitions must be greater than 0.", nameof(numberOfPartitions));

            _numberOfPartitions = numberOfPartitions;
        }

        public int GetPartition(byte[] value)
        {
            var hash = Math.Abs(Hash(value));
            return (int)(hash % _numberOfPartitions);
        }

        private static long Hash(byte[] value)
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
}
