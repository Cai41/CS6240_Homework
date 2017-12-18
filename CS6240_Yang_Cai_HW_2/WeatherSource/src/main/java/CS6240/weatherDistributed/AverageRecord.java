package CS6240.weatherDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Class to hold the average temperture
 * @author caiyang
 *
 */
public class AverageRecord implements Writable {
	private double minAve = Double.MAX_VALUE;
	private double maxAve = Double.MAX_VALUE;

	public void write(DataOutput out) throws IOException {
		out.writeDouble(minAve);
		out.writeDouble(maxAve);
	}

	public void readFields(DataInput in) throws IOException {
		minAve = in.readDouble();
		maxAve = in.readDouble();
	}
	
	public static AverageRecord read(DataInput in) throws IOException {
		AverageRecord w = new AverageRecord();
		w.readFields(in);
		return w;
	}

	public double getMinAve() {
		return minAve;
	}

	public void setMinAve(Double minAve) {
		this.minAve = minAve;
	}

	public double getMaxAve() {
		return maxAve;
	}

	public void setMaxAve(Double maxAve) {
		this.maxAve = maxAve;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(minAve == Double.MAX_VALUE ? "NA" : minAve)
		.append(", ")
		.append(maxAve == Double.MAX_VALUE ? "NA" : maxAve);
		return sb.toString();
	}

}
