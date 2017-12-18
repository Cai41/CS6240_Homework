package cs6240.pagerank;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Use PriorityQueue to keep only highest 100 pages
 * @author caiyang
 *
 */
public class TopKMapper extends Mapper<Text, Text, Text, RankNodeWritable> {
	private Text id = new Text(Constants.DUMMY);
	private PriorityQueue<RankNodeWritable> pq;

	public void setup(Context context) throws IOException, InterruptedException {
		pq = new PriorityQueue<>((n1, n2) -> Double.compare(n1.getRank(), n2.getRank()));
	}

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		NodeWritable nw = NodeWritable.fromString(line);
		pq.offer(new RankNodeWritable(key.toString(), nw.getRank()));
		// only keeps top k elements, discard the (k+1)th element.
		if (pq.size() > Constants.TOP_K) pq.poll();
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		// emit top K elements within one mapper task
		for (RankNodeWritable rnw : pq) {
			context.write(id, rnw);
		}
	}
}
