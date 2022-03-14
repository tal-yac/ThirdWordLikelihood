package step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<BigramKey, IntWritable> {

	// ensure that keys with same w3 are directed to the same reducer
	@Override
	public int getPartition(BigramKey key, IntWritable value, int numPartitions) {
		if (key.isEmpty())
			return numPartitions - 1;
		return Math.abs(key.getWord3().toString().hashCode()) % (numPartitions - 1);
	}

}