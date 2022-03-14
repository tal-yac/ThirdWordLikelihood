package step4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerClass extends Partitioner<SortingKey, Text> {

	// ensure that keys with same w1 are directed to the same reducer
	@Override
	public int getPartition(SortingKey key, Text value, int numPartitions) {
		return Math.abs(key.getW1().hashCode()) % numPartitions;
	}
}