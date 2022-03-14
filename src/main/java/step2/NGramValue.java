package step2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class NGramValue implements Writable {

	private IntWritable occurrences;
	private IntWritable occurrencesOfW3;

	public NGramValue(int occurrences, int occurrencesOfW3) {
		this.occurrences = new IntWritable(occurrences);
		this.occurrencesOfW3 = new IntWritable(occurrencesOfW3);
	}
	
	public NGramValue() {
	    this(0, 0);
    }

	public IntWritable getOccurrences() {
		return occurrences;
	}

	public IntWritable getOccurrencesOfW3() {
		return this.occurrencesOfW3;
	}

	public void readFields(DataInput in) throws IOException {
		this.occurrences.readFields(in);
		this.occurrencesOfW3.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.occurrences.write(out);
		this.occurrencesOfW3.write(out);
	}

}
