package CS6240.weatherDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.io.Writable;

/**
 * Hold the final output for reducer, including average temperature for each year.
 * @author caiyang
 *
 */
public class SecondarySortAverageRecords implements Writable{
	/** Entry hold average temperature for a single year
	 * 
	 */
	public static class Entry implements Writable {
		int year;
		double minAve = Double.MAX_VALUE;
		double maxAve = Double.MAX_VALUE;
		
		public void setYear(int year) {
			this.year = year;
		}
		
		public void setMinAve(Double minAve) {
			this.minAve = minAve;
		}

		public void setMaxAve(Double maxAve) {
			this.maxAve = maxAve;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(year);
			out.writeDouble(minAve);
			out.writeDouble(maxAve);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			year = in.readInt();
			minAve = in.readDouble();
			maxAve = in.readDouble();
		}
		
		public static Entry read(DataInput in) throws IOException {
			Entry e = new Entry();
			e.readFields(in);
			return e;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder("(");
			sb.append(year)
			.append(", ")
			.append(minAve == Double.MAX_VALUE ? "NA" : minAve)
			.append(", ")
			.append(maxAve == Double.MAX_VALUE ? "NA" : maxAve)
			.append(")");
			return sb.toString();
		}
	
	}

	// Average temperture for each single year
	private List<Entry> list = new ArrayList<>();
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(list.size());
		for (Entry e : list) e.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		list = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			list.add(Entry.read(in));
		}	
	}

	public void setList(List<Entry> l) {
		list = l;
	}

	public String toString() {
		StringJoiner sj = new StringJoiner(", ", "[", "]");
		for (Entry e : list) sj.add(e.toString());
		return sj.toString();
	}
}
