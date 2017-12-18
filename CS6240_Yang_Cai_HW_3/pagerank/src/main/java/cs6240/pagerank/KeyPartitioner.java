package cs6240.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Each reduce task/partition should receive its own dummy node.
 * @author caiyang
 *
 */
public class KeyPartitioner extends Partitioner<Text, NodeWritable> {

    @Override
    public int getPartition(Text key, NodeWritable value, int numPartitions) {      
      if (key.toString().startsWith(Constants.DUMMY)) {
    	  int part = Integer.parseInt(key.toString().substring(3));
    	  return part;
      }
      return Math.abs(key.hashCode()) % numPartitions;
    }
  }
