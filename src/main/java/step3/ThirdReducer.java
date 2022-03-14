package step3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<BigramKey, NGramValue, Text, Text> {

	IntWritable w1w2oc = new IntWritable(0);
	IntWritable c0 = new IntWritable(0);

	@Override
	public void reduce(BigramKey key, Iterable<NGramValue> values, Context context)
			throws IOException, InterruptedException {
		int sumoc = 0;
		int w2oc = 0;
		int w3oc = 0;
		int w2w3oc = 0;
		for (NGramValue value : values) {
			sumoc += value.getOccurrences().get();
			if (key.isThreeGram()) {
				w2oc += value.getOccurrencesOfW2().get();
				w3oc += value.getOccurrencesOfW3().get();
				w2w3oc += value.getOccurrencesOfW2W3().get();
			}
		}
		if (key.isEmpty()) {
			c0.set(sumoc);
			return;
		}
		if (!key.isThreeGram()) {
			w1w2oc.set(sumoc);
			return;
		}
		NGramValue val = new NGramValue(sumoc, w2oc, w3oc, w2w3oc);
		double prob = calculateProb(val);
		context.write(new Text(key.toString()), new Text("" + prob));
	}

	private double calculateProb(NGramValue occs) {
		int n1, n2, n3, c1, c2;
		double k2, k3;
		n1 = occs.getOccurrencesOfW3().get();
		n2 = occs.getOccurrencesOfW2W3().get();
		n3 = occs.getOccurrences().get();
		c1 = occs.getOccurrencesOfW2().get();
		c2 = w1w2oc.get();
		k2 = (Math.log10(n2 + 1) + 1) / (Math.log10(n2 + 1) + 1);
		k3 = (Math.log10(n3 + 1) + 1) / (Math.log10(n3 + 1) + 2);
		return k3 * n3 / c2 + (1 - k3) * k2 * n2 / c1 + (1 - k3) * (1 - k2) * n1 / c0.get();
	}

}
