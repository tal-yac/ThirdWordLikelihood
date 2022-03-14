package step4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SortingKey implements WritableComparable<SortingKey> {

	private Text w1, w2, w3, likelihood;

	public SortingKey(String w1, String w2, String w3, double likelihood) {
		this.w1 = new Text(w1);
		this.w2 = new Text(w2);
		this.w3 = new Text(w3);
		this.likelihood = new Text("" + likelihood);
	}

	public SortingKey() {
		this("", "", "", 0.0);
	}

	public void readFields(DataInput in) throws IOException {
		w1.readFields(in);
		w2.readFields(in);
		w3.readFields(in);
		likelihood.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		w1.write(out);
		w2.write(out);
		w3.write(out);
		likelihood.write(out);
	}

	@Override
	public int compareTo(SortingKey o) {
		int ret = w1.toString().compareTo(o.w1.toString());
		if (ret == 0) {
			ret = w2.toString().compareTo(o.w2.toString());
			if (ret == 0)
				return -Double.compare(getLikelihood(), o.getLikelihood());
		}
		return ret;
	}

	@Override
	public String toString() {
		return w1 + " " + w2 + " " + w3;
	}

	public double getLikelihood() {
		return Double.valueOf(likelihood.toString());
	}

	public String getW1() {
		return w1.toString();
	}

	public String getW2() {
		return w2.toString();
	}

	public String getW3() {
		return w3.toString();
	}

}