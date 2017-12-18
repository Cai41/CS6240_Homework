package CS6240.weatherDistributed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper mapping each line to a single accumulate struture.
 * @author caiyang
 *
 */
public class TemperatureMapper extends Mapper<Object, Text, Text, RecordWritable> {
	private Text word = new Text();
	private RecordWritable rw = new RecordWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split(",");
		if (!strs[2].equals("TMAX") && !strs[2].equals("TMIN")) return;
		word.set(strs[0]);
		long temp = Long.parseLong(strs[3]);
		if (strs[2].equals("TMAX")) {
			rw.setMaxCount(1);
			rw.setMaxSum(temp);
			rw.setMinCount(0);
			rw.setMinSum(0);
		} else {
			rw.setMaxCount(0);
			rw.setMaxSum(0);
			rw.setMinCount(1);
			rw.setMinSum(temp);
		}
		context.write(word, rw);
	}
}
