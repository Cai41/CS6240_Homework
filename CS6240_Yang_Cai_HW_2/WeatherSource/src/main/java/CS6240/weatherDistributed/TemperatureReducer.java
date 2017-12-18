package CS6240.weatherDistributed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for computing average temperature
 * @author caiyang
 *
 */
public class TemperatureReducer extends Reducer<Text, RecordWritable, Text, AverageRecord> {
	private AverageRecord result = new AverageRecord();

	public void reduce(Text key, Iterable<RecordWritable> values, Context context)
			throws IOException, InterruptedException {
		long maxSum = 0, maxCount = 0, minCount = 0, minSum = 0;
		// accumulate all the values
		for (RecordWritable val : values) {
			maxSum += val.getMaxSum();
			minSum += val.getMinSum();
			maxCount += val.getMaxCount();
			minCount += val.getMinCount();
		}
		// if the count is zero, set the average to Double.MAX_VALUE, it will be converted
		// to N.A in toStirng() method
		result.setMaxAve(maxCount == 0 ? Double.MAX_VALUE : maxSum * 1.0 / maxCount);
		result.setMinAve(minCount == 0 ? Double.MAX_VALUE : minSum * 1.0 / minCount);
		context.write(key, result);
	}
}
