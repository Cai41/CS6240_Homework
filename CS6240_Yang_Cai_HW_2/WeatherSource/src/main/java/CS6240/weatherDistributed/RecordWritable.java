package CS6240.weatherDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Hold the accumulate temperature information for a single station
 * @author caiyang
 *
 */
public class RecordWritable implements Writable {
	private long maxSum;
	private long minSum;
	private long maxCount;
	private long minCount;

	public void write(DataOutput out) throws IOException {
		out.writeLong(maxSum);
		out.writeLong(minSum);
		out.writeLong(maxCount);
		out.writeLong(minCount);
	}

	public void readFields(DataInput in) throws IOException {
		maxSum = in.readLong();
		minSum = in.readLong();
		maxCount = in.readLong();
		minCount = in.readLong();
	}
	
	public static RecordWritable read(DataInput in) throws IOException {
		RecordWritable w = new RecordWritable();
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
}
