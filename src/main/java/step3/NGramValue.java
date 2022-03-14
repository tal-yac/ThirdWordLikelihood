package step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class NGramValue implements Writable {

	private IntWritable occurrences;
	private IntWritable occurrencesOfW2;
	private IntWritable occurrencesOfW3;
	private IntWritable occurrencesOfW2W3;

	public NGramValue(int occurrences, int occurrencesOfW2, int occurrencesOfW3,
			int occurrencesOfW2W3) {
		this.occurrences = new IntWritable(occurrences);
		this.occurrencesOfW2 = new IntWritable(occurrencesOfW2);
		this.occurrencesOfW3 = new IntWritable(occurrencesOfW3);
		this.occurrencesOfW2W3 = new IntWritable(occurrencesOfW2W3);
	}

	public NGramValue() {
		this(0, 0, 0, 0);
	}

	public IntWritable getOccurrences() {
		return occurrences;
	}

	public IntWritable getOccurrencesOfW2() {
		return occurrencesOfW2;
	}

	public IntWritable getOccurrencesOfW3() {
		return occurrencesOfW3;
	}

	public IntWritable getOccurrencesOfW2W3() {
		return occurrencesOfW2W3;
	}

	public void readFields(DataInput in) throws IOException {
		occurrences.readFields(in);
		occurrencesOfW2.readFields(in);
		occurrencesOfW3.readFields(in);
		occurrencesOfW2W3.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		occurrences.write(out);
		occurrencesOfW2.write(out);
		occurrencesOfW3.write(out);
		occurrencesOfW2W3.write(out);
	}

}
