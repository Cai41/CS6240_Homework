package cs6240.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Final output, each RankNodeWritable represents one page among top K pages.
 * @author caiyang
 *
 */
public class RankNodeWritable implements Writable {
	private String key;
	private double rank;
	
	public RankNodeWritable() {
		//NO-OP
	}
	
	public RankNodeWritable(String key, double rank) {
		this.key = key;
		this.rank = rank;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeDouble(rank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		rank = in.readDouble();
	}
	
	public String toString() {
		return key + ": " + rank;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}
	
}
