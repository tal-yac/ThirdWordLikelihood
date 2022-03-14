package step1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, BigramKey, IntWritable> {

	private static final char FIRST_HEB_CHAR = (char) 1488;
	private static final char LAST_HEB_CHAR = (char) 1514;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// value: n-gram \t year \t occurrences \t volume_count \n
		// split the value using tab delimiter
		String[] splitted = value.toString().split("\t");

		// parse the n-gram
		String ngram = splitted[0];

		// filter only ngrams that contain non-alphabetic characters
		if (!onlyLettersAndSpace(ngram)) {
			return;
		}

		// parse the occurrences
		IntWritable occurrences = new IntWritable(Integer.parseInt(splitted[2]));

		// split ngram into words
		String[] words = ngram.split(" ");

		if (words.length != 3)
			return;

		context.write(new BigramKey(), occurrences);
		context.write(new BigramKey(words[0]), occurrences);
		context.write(new BigramKey(words[0], words[1]), occurrences);
		context.write(new BigramKey(words[0], words[1], words[2]), occurrences);
	}

	private static boolean isLetterOrSpace(char c) {
		return ((FIRST_HEB_CHAR <= c && c <= LAST_HEB_CHAR) || c == ' ');
	}

	private static boolean onlyLettersAndSpace(String string) {
		for (int i = 0; i < string.length(); i++) {
			if (!isLetterOrSpace(string.charAt(i))) {
				return false;
			}
		}
		return true;
	}
}