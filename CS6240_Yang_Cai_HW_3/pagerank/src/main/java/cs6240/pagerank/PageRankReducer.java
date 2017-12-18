package cs6240.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Calculate new dangling nodes' sum and new page rank for each node.
 * @author caiyang
 *
 */
public class PageRankReducer extends Reducer<Text, NodeWritable, Text, NodeWritable> {
	private double delta = 0.0;

	public void reduce(Text key, Iterable<NodeWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0, alpha = context.getConfiguration().getDouble(Constants.ALPHA, Constants.ALPHA_VALUE);
		long totalNodes = context.getConfiguration().getLong(Constants.TOTAL_NODES, 1L);
		NodeWritable nw = new NodeWritable();
		// if it dangling node, add to delta, so by KeyComparator, 
		// before any nodes are processed, we already have delta calculated
		if (key.toString().startsWith(Constants.DUMMY)) {
			for (NodeWritable val : values) {
				delta += val.getRank();
			}
			return;
		}

		// calculate new page rank
		for (NodeWritable val : values) {
			if (val.getRank() < 0) {
				nw.setAdj(val.getAdj());
			} else {
				sum += val.getRank();
			}
		}
		sum += delta / totalNodes;
		nw.setRank(alpha / totalNodes + (1 - alpha) * sum);
		context.write(key, nw);
	}
}
