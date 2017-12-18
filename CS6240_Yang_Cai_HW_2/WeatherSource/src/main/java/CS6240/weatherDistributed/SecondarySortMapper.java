package CS6240.weatherDistributed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for secondary sort.
 * @author caiyang
 *
 */
public class SecondarySortMapper extends Mapper<Object, Text, SecondarySortKeyWritable, SecondarySortValueWritable> {
	private SecondarySortKeyWritable syp = new SecondarySortKeyWritable();
	private SecondarySortValueWritable yrw = new SecondarySortValueWritable();
	
	/**
	 * For each valid line, we use secondary sort to move the year information into key
	 * So we will emit((station, key), (year, maxSum, maxCt, minSum, minCt)) for each
	 * valid record.
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split(",");
		if (!strs[2].equals("TMAX") && !strs[2].equals("TMIN")) return;
		int year = Integer.parseInt(strs[1].substring(0, 4));
		syp.getStation().set(strs[0]);
		syp.getYear().set(year);
		yrw.setYear(year);
		long temp = Long.parseLong(strs[3]);
		if (strs[2].equals("TMAX")) {
			yrw.setMaxCount(1L);
			yrw.setMaxSum(temp);
			yrw.setMinCount(0L);
			yrw.setMinSum(0L);
		} else {
			yrw.setMaxCount(0L);
			yrw.setMaxSum(0L);
			yrw.setMinCount(1L);
			yrw.setMinSum(temp);
		}
		context.write(syp, yrw);
	}
}
