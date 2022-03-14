package step1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<BigramKey, IntWritable, Text, Text> {

	IntWritable w3oc = new IntWritable(0);

	@Override
	public void reduce(BigramKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		if (key.isOneGram())
			w3oc.set(sum);
		context.write(new Text(key.toString()), new Text(sum + " " + w3oc));
	}

}
