package CS6240.weatherDistributed;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner for secondary sort, based only on station id. 
 * All the records for same partition goes to same reducer.
 * @author caiyang
 *
 */
public class SecondarySortKeyPartitioner extends Partitioner<SecondarySortKeyWritable, SecondarySortValueWritable>{

	@Override
	public int getPartition(SecondarySortKeyWritable key, SecondarySortValueWritable value, int numPartitions) {
		return Math.abs(key.getStation().hashCode()) % numPartitions;
	}
}
