package cs6240.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs6240.pagerank.PageRank.MyCounters;

/**
 * Emit all outlinks from one page.
 * @author caiyang
 *
 */
public class PreProcessReducer extends Reducer<Text, NodeWritable, Text, NodeWritable> {
	private Text id = new Text();
	private NodeWritable nw = new NodeWritable();
	
	public void reduce(Text key, Iterable<NodeWritable> values, Context context)
			throws IOException, InterruptedException {
		String pageName = key.toString();
		id.set(pageName);
		context.getCounter(MyCounters.TotalNodes).increment(1);
		List<String> adj = new ArrayList<>();
		for (NodeWritable v : values) {
			adj.addAll(v.getAdj());
		}
		// remove self-link
		while(adj.remove(pageName));
		nw.setAdj(adj);
		context.write(id, nw);
	}
}
