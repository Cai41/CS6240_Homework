package CS6240.weatherDistributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Mapper's output key in secondary sort.
 * @author caiyang
 *
 */
public class SecondarySortKeyWritable implements WritableComparable<SecondarySortKeyWritable> {
	private Text station = new Text();
	private IntWritable year = new IntWritable();

	@Override
	public void write(DataOutput out) throws IOException {
		station.write(out);
		year.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		station.readFields(in);
		year.readFields(in);
	}

	@Override
	/**
	 * We sort the keys first based on station id, and then by year within one station id,
	 * in increasing order.
	 */
	public int compareTo(SecondarySortKeyWritable o) {
		int res = station.compareTo(o.station);
		if (res == 0) {
			res = year.compareTo(o.year);
		}
		return res;
	}

	public Text getStation() {
		return station;
	}

	public void setStation(Text station) {
		this.station = station;
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = year;
	}

}
