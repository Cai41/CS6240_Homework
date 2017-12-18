package cs6240.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Using order inversion, ensure that dummy nodes always come before any other nodes.
 * @author caiyang
 *
 */
public class KeyComparator extends WritableComparator {
	protected KeyComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text key1 = (Text) w1;
		Text key2 = (Text) w2;
		int res = key1.compareTo(key2);
		// if key1/key2 is dummy node, then key1/key2 should be the smaller one.
		if (res != 0 && key1.toString().startsWith(Constants.DUMMY)) return -1;
		if (res != 0 && key2.toString().startsWith(Constants.DUMMY)) return 1;
		return res;
	}
}
