package join.mr;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class NaturalKeyPartitioner extends Partitioner<CompositeKey,Text> {

	@Override
	public int getPartition(CompositeKey compositeKey, Text text, int numPartitions) {
		return compositeKey.getId().hashCode() % numPartitions;
	}
}