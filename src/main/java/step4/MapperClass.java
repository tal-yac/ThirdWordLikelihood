package step4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass
		extends Mapper<Text, Text, SortingKey, Text> {

	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] words = key.toString().split(" ");
		String w1 = words[0];
		String w2 = words[1];
		String w3 = words[2];

		SortingKey sortingKey = new SortingKey(w1, w2, w3, Double.valueOf(value.toString()));

		// Send to context for sorting
		context.write(sortingKey, value);
	}
}