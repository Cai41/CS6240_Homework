package CS6240.weatherDistributed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner to combine each station's information into one accumulate structure.
 * @author caiyang
 *
 */
public class TemperatureCombiner extends Reducer<Text, RecordWritable, Text, RecordWritable> {
	private RecordWritable result = new RecordWritable();

	public void reduce(Text key, Iterable<RecordWritable> values, Context context)
			throws IOException, InterruptedException {
		long maxSum = 0, maxCount = 0, minCount = 0, minSum = 0;
		for (RecordWritable val : values) {
			maxSum += val.getMaxSum();
			minSum += val.getMinSum();
			maxCount += val.getMaxCount();
			minCount += val.getMinCount();
		}
		result.setMaxCount(maxCount);
		result.setMinCount(minCount);
		result.setMaxSum(maxSum);
		result.setMinSum(minSum);
		context.write(key, result);
	}
}
