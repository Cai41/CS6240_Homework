package CS6240.weatherDistributed;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper using in-mapper combiner to combine all the information for stations,
 * and emit when cleanUp() is called.
 * @author caiyang
 *
 */
public class TemperatureCombiningMapper extends Mapper<Object, Text, Text, RecordWritable> {
	private Text word = new Text();
	private Map<String, RecordWritable> combiner;

	public void setup(Context context) throws IOException, InterruptedException {
		combiner = new HashMap<>();
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split(",");
		if (!strs[2].equals("TMAX") && !strs[2].equals("TMIN"))
			return;
		if (!combiner.containsKey(strs[0]))
			combiner.put(strs[0], new RecordWritable());
		long temp = Long.parseLong(strs[3]);
		// store the information in hashmap
		if (strs[2].equals("TMAX")) {
			combiner.get(strs[0]).setMaxSum(combiner.get(strs[0]).getMaxSum() + temp);
			combiner.get(strs[0]).setMaxCount(combiner.get(strs[0]).getMaxCount() + 1);
		} else {
			combiner.get(strs[0]).setMinSum(combiner.get(strs[0]).getMinSum() + temp);
			combiner.get(strs[0]).setMinCount(combiner.get(strs[0]).getMinCount() + 1);
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// emit record for each station id
		for (Map.Entry<String, RecordWritable> e : combiner.entrySet()) {
			word.set(e.getKey());
			context.write(word, e.getValue());
		}
	}
}