package step3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import main.Config;

public class BigramKey implements WritableComparable<BigramKey> {

	private Text tag;
	private Text w1;
	private Text w2;
	private Text w3;

	public BigramKey(String first, String second, String third, int tag) {
		this.w1 = new Text(first);
		this.w2 = new Text(second);
		this.w3 = new Text(third);
		this.tag = new Text("" + tag);
	}

	public BigramKey(String w1, String w2, String w3) {
		this(w1, w2, w3, -1);
	}

	public BigramKey(String w1, String w2) {
		this(w1, w2, Config.WILDCARD, -1);
	}

	public BigramKey(int tag) {
		this(Config.WILDCARD, Config.WILDCARD, Config.WILDCARD, tag);
	}

	public BigramKey() {
		this(Config.WILDCARD, Config.WILDCARD, Config.WILDCARD, 0);
	}

	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		w1.readFields(in);
		w2.readFields(in);
		w3.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		tag.write(out);
		w1.write(out);
		w2.write(out);
		w3.write(out);
	}

	public int compareTo(BigramKey other) {
		if (getTag() > 0)
			return -1;
		int res = w1.toString().compareTo(other.w1.toString());
		if (res == 0) {
			if (w2.toString().equals(Config.WILDCARD)
					&& !other.w2.toString().equals(Config.WILDCARD))
				res = -1;
			else if (other.w2.toString().equals(Config.WILDCARD)
					&& !w2.toString().equals(Config.WILDCARD))
				res = 1;
			else
				res = w2.toString().compareTo(other.w2.toString());
		}
		if (res == 0) { // same w1 and same w2
			if (w3.toString().equals(Config.WILDCARD)
					&& !other.w3.toString().equals(Config.WILDCARD))
				res = -1;
			else if (other.w3.toString().equals(Config.WILDCARD)
					&& !w3.toString().equals(Config.WILDCARD))
				res = 1;
			else
				res = w3.toString().compareTo(other.w3.toString());
		}
		return res;
	}

	public Text getWord1() {
		return w1;
	}

	public Text getWord2() {
		return w2;
	}

	public Text getWord3() {
		return w3;
	}

	public boolean isEmpty() {
		return w1.toString().equals(Config.WILDCARD)
				&& w2.toString().equals(Config.WILDCARD)
				&& w3.toString().equals(Config.WILDCARD);
	}

	public boolean isThreeGram() {
		return !w3.toString().equals(Config.WILDCARD);
	}

	public boolean isTwoGram() {
		return w3.toString().equals(Config.WILDCARD);
	}

	public int getTag() {
		return Integer.valueOf(tag.toString());
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BigramKey))
			return false;
		BigramKey other = (BigramKey) obj;
		if (getTag() >= 0 || other.getTag() >= 0)
			return false;
		return w1.toString().equals(other.w1.toString())
				&& w2.toString().equals(other.w2.toString())
				&& w3.toString().equals(other.w3.toString());
	}

	@Override
	public String toString() {
		return w1 + " " + w2 + " " + w3;
	}
}
