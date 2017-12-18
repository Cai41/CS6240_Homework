package CS6240.weatherDistributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WeatherDistributed {
	public enum RunMode{
		NoCombiner,
		WithCombiner,
		InMapperCombiner,
		SecondarySort;
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set(TextOutputFormat.SEPERATOR, ", ");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: CS6240.weatherDistributed.WeatherDistributed <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "weather mapreduce");
		job.setJarByClass(WeatherDistributed.class);
		// run the corresponding program based on the command line arguments
		switch (RunMode.valueOf(otherArgs[0])) {
		case NoCombiner:
			job.setMapperClass(TemperatureMapper.class);
			job.setReducerClass(TemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(RecordWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(AverageRecord.class);
			break;
		case WithCombiner:
			job.setMapperClass(TemperatureMapper.class);
			job.setCombinerClass(TemperatureCombiner.class);
			job.setReducerClass(TemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(RecordWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(AverageRecord.class);
			break;
		case InMapperCombiner:
			job.setMapperClass(TemperatureCombiningMapper.class);
			job.setReducerClass(TemperatureReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(RecordWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(AverageRecord.class);
			break;
		case SecondarySort:
			job.setMapperClass(SecondarySortMapper.class);
			job.setPartitionerClass(SecondarySortKeyPartitioner.class);
			job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
			job.setReducerClass(SecondarySortReducer.class);
			job.setMapOutputKeyClass(SecondarySortKeyWritable.class);
			job.setMapOutputValueClass(SecondarySortValueWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			break;
		default:
			System.err.println("Unrecognized mode");
			return;
		}
		for (int i = 1; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
