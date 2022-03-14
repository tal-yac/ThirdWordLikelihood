package step4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<SortingKey, Text, Text, DoubleWritable> {

	@Override
	public void reduce(SortingKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.toString()), new DoubleWritable(key.getLikelihood()));
	}

}