package step3;

import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<BigramKey, NGramValue> {

	// ensure that keys with same w1 are directed to the same reducer
	@Override
	public int getPartition(BigramKey key, NGramValue value, int numPartitions) {
		if (key.isEmpty())
			return key.getTag() % numPartitions;
		return Math.abs(key.getWord1().toString().hashCode()) % numPartitions;
	}

}