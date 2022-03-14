package step2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<BigramKey, NGramValue, Text, Text> {

	IntWritable w2oc = new IntWritable(0);
	IntWritable w2w3oc = new IntWritable(0);

	@Override
	public void reduce(BigramKey key, Iterable<NGramValue> values, Context context)
			throws IOException, InterruptedException {
		int sumoc = 0;
		int w3oc = 0;
		for (NGramValue value : values) {
			sumoc += value.getOccurrences().get();
			if (key.isThreeGram())
				w3oc += value.getOccurrencesOfW3().get();
		}
		if (key.isThreeGram() || key.isEmpty()) {
			context.write(new Text(key.toString()),
					new Text(sumoc + " " + w2oc + " " + w3oc + " " + w2w3oc));
			return;
		}
		if (key.isOneGram()) {
			w2oc.set(sumoc);
			return;
		}
		w2w3oc.set(sumoc);
		context.write(new Text(key.toString()),
				new Text(sumoc + " " + 0 + " " + 0 + " " + 0));
	}
	
}
