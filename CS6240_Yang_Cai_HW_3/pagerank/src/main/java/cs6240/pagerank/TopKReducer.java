package cs6240.pagerank;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Select top K pages that has highest page rank from all pages.
 * @author caiyang
 *
 */
public class TopKReducer extends Reducer<Text, RankNodeWritable, IntWritable, RankNodeWritable> {
	private PriorityQueue<RankNodeWritable> pq;
	private IntWritable iw = new IntWritable();

	public void setup(Context context) throws IOException, InterruptedException {
		pq = new PriorityQueue<>((n1, n2) -> Double.compare(n1.getRank(), n2.getRank()));
	}

	public void reduce(Text key, Iterable<RankNodeWritable> values, Context context)
			throws IOException, InterruptedException {
		for (RankNodeWritable rnw : values) {
			pq.offer(new RankNodeWritable(rnw.getKey(), rnw.getRank()));
			// only keeps top k elements, discard the (k+1)th element.
			if (pq.size() > Constants.TOP_K) pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		Stack<RankNodeWritable> stk = new Stack<>();
		while (!pq.isEmpty()) {
			stk.push(pq.poll());
		}
		// emit all pages in decreasing order
		while (!stk.empty()) {
			context.write(iw, stk.pop());
			iw.set(iw.get() + 1);
		}
	}
}
