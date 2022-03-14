package step2;

import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<BigramKey, NGramValue> {

	// ensure that keys with same w2 are directed to the same reducer
	@Override
	public int getPartition(BigramKey key, NGramValue value, int numPartitions) {
		return Math.abs(key.getWord2().toString().hashCode()) % numPartitions;
	}

}