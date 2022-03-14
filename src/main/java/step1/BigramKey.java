package step1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import main.Config;

public class BigramKey implements WritableComparable<BigramKey> {

	private Text first;
	private Text second;
	private Text third;

	public BigramKey(String first, String second, String third) {
		this.first = new Text(first);
		this.second = new Text(second);
		this.third = new Text(third);
	}

	public BigramKey(String w3) {
		this(Config.WILDCARD, Config.WILDCARD, w3);
	}

	public BigramKey(String w2, String w3) {
		this(Config.WILDCARD, w2, w3);
	}

	public BigramKey() {
		this(Config.WILDCARD, Config.WILDCARD, Config.WILDCARD);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	public int compareTo(BigramKey other) {
		int res = this.third.toString().compareTo(other.third.toString());
		if (res == 0) { // same w3
			if (this.second.toString().equals("*")
					&& !other.second.toString().equals("*"))
				res = -1;
			else if (other.second.toString().equals("*")
					&& !this.second.toString().equals("*"))
				res = 1;
			else
				res = this.second.toString().compareTo(other.second.toString());
		}
		if (res == 0) { // same w2 and same w3
			if (first.toString().equals("*") && !other.first.toString().equals("*"))
				res = -1;
			else if (other.first.toString().equals("*")
					&& !this.first.toString().equals("*"))
				res = 1;
			else
				res = this.first.toString().compareTo(other.first.toString());
		}
		return res;
	}

	public Text getWord1() {
		return this.first;
	}

	public Text getWord2() {
		return this.second;
	}

	public Text getWord3() {
		return third;
	}

	public boolean isEmpty() {
		return first.toString().equals(Config.WILDCARD)
				&& second.toString().equals(Config.WILDCARD)
				&& third.toString().equals(Config.WILDCARD);
	}

	public boolean isOneGram() {
		return first.toString().equals(Config.WILDCARD)
				&& second.toString().equals(Config.WILDCARD);
	}

	public boolean isTwoGram() {
		return first.toString().equals(Config.WILDCARD);
	}

	public boolean isThreeGram() {
		return !first.toString().equals(Config.WILDCARD);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BigramKey))
			return false;
		BigramKey other = (BigramKey) obj;
		return first.toString().equals(other.first.toString())
				&& second.toString().equals(other.second.toString())
				&& third.toString().equals(other.third.toString());
	}

	@Override
	public String toString() {
		if (isEmpty() || isThreeGram())
			return first + " " + second + " " + third;
		if (isOneGram())
			return third.toString();
		return second + " " + third;
	}
}
