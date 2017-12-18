package cs6240.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Rank and adjacent list of a page.
 * @author caiyang
 *
 */
public class NodeWritable implements Writable {
	private double rank;
	private List<String> adj;
	
	public NodeWritable() {
		adj = new ArrayList<>();
	}
	
	public NodeWritable(double rank, List<String> adj) {
		this.rank = rank;
		this.adj = adj;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(rank);
		if (adj == null) {
			out.writeInt(0);
		} else {
			out.writeInt(adj.size());
			for (int i = 0; i < adj.size(); i++) {
				out.writeUTF(adj.get(i));
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rank = in.readDouble();
		adj = new ArrayList<>();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			adj.add(in.readUTF());
		}
	}
	
	public List<String> getAdj() {
		return adj;
	}

	public void setAdj(List<String> adj) {
		this.adj = adj;
	}

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}
	
	public String toString() {
		return rank + ": " + adj;
	}

	/**
	 * Create a new NodeWritable from string
	 * @param line
	 * @return
	 */
	static public NodeWritable fromString(String line) {
		NodeWritable nw = new NodeWritable();
		int delimLoc = line.indexOf(':');
		nw.setRank(Double.parseDouble(line.substring(0, delimLoc)));
		String rest = line.substring(delimLoc + 2);
		if (rest.length() == 2) return nw;  //empty list: "[]"
		nw.setAdj(Arrays.asList(rest.substring(1, rest.length() - 1).split(", ")));
		return nw;
	}

}
