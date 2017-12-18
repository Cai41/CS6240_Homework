package CS6240.weatherDistributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import CS6240.weatherDistributed.SecondarySortAverageRecords.Entry;

/**
 * Reducer for secondary sort. Since we have the grouping comparator, and the keys are sorted
 * by station and year, so for each reduce call, all the records of one station are in the
 * list, and they are sorted in increasing order of year.
 * @author caiyang
 *
 */
public class SecondarySortReducer extends Reducer<SecondarySortKeyWritable, SecondarySortValueWritable, Text, SecondarySortAverageRecords> {
	private SecondarySortAverageRecords result = new SecondarySortAverageRecords();
	private Text station = new Text();
	
	public void reduce(SecondarySortKeyWritable key, Iterable<SecondarySortValueWritable> values, Context context)
			throws IOException, InterruptedException {
		long maxSum = 0, maxCount = 0, minCount = 0, minSum = 0;
		Integer year = null;
		// hold all the average temperatures of each year
		List<Entry> lst = new ArrayList<>();
		for (SecondarySortValueWritable val : values) {
			// if it is different year from previous one, then we calculate the 
			// averages and append to list
			if (year != null && val.getYear() != year) {
				Entry e = new Entry();
				e.setYear(year);
				e.setMaxAve(maxCount == 0 ? Double.MAX_VALUE : maxSum * 1.0 / maxCount);
				e.setMinAve(minCount == 0 ? Double.MAX_VALUE : minSum * 1.0 / minCount);
				lst.add(e);
				maxCount = 0;
				maxSum = 0;
				minCount = 0;
				minSum = 0;
			}
			// otherwise, accumulate the data.
			year = val.getYear();
			maxSum += val.getMaxSum();
			minSum += val.getMinSum();
			maxCount += val.getMaxCount();
			minCount += val.getMinCount();
		}
		// append the last year's information to list
		Entry e = new Entry();
		e.setYear(year);
		e.setMaxAve(maxCount == 0 ? Double.MAX_VALUE : maxSum * 1.0 / maxCount);
		e.setMinAve(minCount == 0 ? Double.MAX_VALUE : minSum * 1.0 / minCount);
		lst.add(e);
		station.set(key.getStation().toString());
		result.setList(lst);
		context.write(station, result);
	}
}
