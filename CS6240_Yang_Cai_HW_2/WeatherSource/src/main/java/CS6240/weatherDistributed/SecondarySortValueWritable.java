package CS6240.weatherDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Mapper's output in secondary sort, including year and temperature.
 * @author caiyang
 *
 */
public class SecondarySortValueWritable implements Writable {
	private int year;
	private long maxSum;
	private long minSum;
	private long maxCount;
	private long minCount;

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeLong(maxSum);
		out.writeLong(minSum);
		out.writeLong(maxCount);
		out.writeLong(minCount);
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		maxSum = in.readLong();
		minSum = in.readLong();
		maxCount = in.readLong();
		minCount = in.readLong();
	}

	public static SecondarySortValueWritable read(DataInput in) throws IOException {
		SecondarySortValueWritable w = new SecondarySortValueWritable();
		w.readFields(in);
		return w;
	}

	public long getMaxSum() {
		return maxSum;
	}

	public long getMinSum() {
		return minSum;
	}

	public long getMaxCount() {
		return maxCount;
	}

	public long getMinCount() {
		return minCount;
	}

	public void setMaxSum(long maxSum) {
		this.maxSum = maxSum;
	}

	public void setMinSum(long minSum) {
		this.minSum = minSum;
	}

	public void setMaxCount(long maxCount) {
		this.maxCount = maxCount;
	}

	public void setMinCount(long minCount) {
		this.minCount = minCount;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}
}
