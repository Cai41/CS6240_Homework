package cs6240.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	public enum MyCounters {
		TotalNodes
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: PageRank <in> [<in>...] <out>");
			System.exit(2);
		}
		String outputPath = otherArgs[otherArgs.length - 1];
		// preprocess the file
		Job job = Job.getInstance(conf, "page rank");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NodeWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/0"));
		job.waitForCompletion(true);
		long totalNodes = job.getCounters().findCounter(MyCounters.TotalNodes).getValue();
		System.out.println("total: " + totalNodes);

		// 10 iteration
		for (int j = 0; j < Constants.RUN_TIMES; j++) {
			job = Job.getInstance(conf, "page rank");
			job.getConfiguration().setLong(Constants.TOTAL_NODES, totalNodes);
			job.getConfiguration().setDouble(Constants.ALPHA, Constants.ALPHA_VALUE);
			// for the first round, every page rank is initialized to 1/totalNodes
			if (j == 0) {
				job.getConfiguration().setBoolean(Constants.FIRST_ROUND, true);
			}
			job.setJarByClass(PageRank.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setSortComparatorClass(KeyComparator.class);
			job.setPartitionerClass(KeyPartitioner.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NodeWritable.class);
			FileInputFormat.addInputPath(job, new Path(outputPath + "/" + j));
			FileOutputFormat.setOutputPath(job, 
					new Path(outputPath + "/" + (j + 1))
					);
			job.waitForCompletion(true);
		}
		
		// select top 100 pages
		job = Job.getInstance(conf, "page rank");
		job.setJarByClass(PageRank.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RankNodeWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RankNodeWritable.class);
		FileInputFormat.addInputPath(job, new Path(outputPath + "/" + Constants.RUN_TIMES));
		FileOutputFormat.setOutputPath(job, 
				new Path(outputPath + "/" + (Constants.RUN_TIMES + 1))
				);
		job.waitForCompletion(true);
	}
}
