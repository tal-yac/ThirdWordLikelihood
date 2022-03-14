package step3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<Text, Text, BigramKey, NGramValue> {

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// key: <w1 w2 w3>
		// value: occurrences w2oc w3-occurrences w2w3oc
		BigramKey newKey;
		String[] keyWords = key.toString().split(" ");
		String[] rawValues = value.toString().split(" ");
		NGramValue newVal = new NGramValue(Integer.parseInt(rawValues[0]),
				Integer.parseInt(rawValues[1]), Integer.parseInt(rawValues[2]),
				Integer.parseInt(rawValues[3]));
		switch (keyWords.length) {
			case 2:
				newKey = new BigramKey(keyWords[0], keyWords[1]);
				break;
			default:
				newKey = new BigramKey(keyWords[0], keyWords[1], keyWords[2]);
				break;
		}
		if (!newKey.isEmpty())
			context.write(newKey, newVal);
		else
			for (int i = 0; i < context.getNumReduceTasks(); i++) {
				context.write(new BigramKey(i), new NGramValue(newVal.getOccurrences().get(), 0, 0, 0));
			}

	}

}