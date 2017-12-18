package CS6240.weatherDistributed;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Grouping partitioner in secondary sort, grouping the records based only on station id.
 * @author caiyang
 *
 */
public class SecondarySortGroupingComparator extends WritableComparator {
	
	public SecondarySortGroupingComparator() {
		super(SecondarySortKeyWritable.class, true);
	}
	
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		SecondarySortKeyWritable sy1 = (SecondarySortKeyWritable)wc1, sy2 = (SecondarySortKeyWritable) wc2;
		return sy1.getStation().compareTo(sy2.getStation());
	}
}
