package step2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<Text, Text, BigramKey, NGramValue> {

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		// key: <w1 w2 w3>
		// value: occurrences w3-occurrences
		BigramKey newKey;
		String[] keyWords = key.toString().split(" ");
		String[] rawValues = value.toString().split(" ");
		NGramValue newVal = new NGramValue(Integer.parseInt(rawValues[0]),
				Integer.parseInt(rawValues[1]));
		switch (keyWords.length) {
			case 1:
				newKey = new BigramKey(keyWords[0]);
				break;
			case 2:
				newKey = new BigramKey(keyWords[0], keyWords[1]);
				break;
			default:
				newKey = new BigramKey(keyWords[0], keyWords[1], keyWords[2]);
				break;
		}
		context.write(newKey, newVal);
	}

}