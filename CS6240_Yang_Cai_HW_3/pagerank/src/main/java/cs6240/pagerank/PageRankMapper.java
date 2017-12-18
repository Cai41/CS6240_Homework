package cs6240.pagerank;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Emit node itself and (outnode, pagerank/numOfOutlinks)
 * For each dangling node, also emit dummy nodes for each partition,
 * inorder to calculate sum of dangling nodes in each Reducer task.
 * @author caiyang
 *
 */
public class PageRankMapper extends Mapper<Text, Text, Text, NodeWritable> {
	private Text id = new Text();
	private NodeWritable val = new NodeWritable();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		NodeWritable nw = NodeWritable.fromString(line);
		long totalNodes = context.getConfiguration().getLong(Constants.TOTAL_NODES, 1L);
		boolean first = context.getConfiguration().getBoolean(Constants.FIRST_ROUND, false);
		double pr = first ? 1.0 / totalNodes : nw.getRank();
		List<String> adj = nw.getAdj();
		context.write(key, new NodeWritable(-1.0, adj));
		if (adj.size() > 0) {
			double p = pr / adj.size();
			val.setRank(p);
			for (int i = 0; i < adj.size(); i++) {
				id.set(adj.get(i));
				context.write(id, val);
			}
		} else {
			val.setRank(pr);
			// emit dummy nodes
			for (int i = 0; i < context.getNumReduceTasks(); i++) {
				id.set(Constants.DUMMY + i);
				context.write(id, val);
			}
		}
	}
}
